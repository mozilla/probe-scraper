import fnmatch
import gzip
from pathlib import Path
from subprocess import check_call
from tempfile import TemporaryDirectory
from typing import Optional, Tuple, Union

TEXT_HTML = "text/html"
APPLICATION_JSON = "application/json"
INDEX_HTML = "index.html"


def _s3_sync(
    src: Union[str, Path],
    dst: Union[str, Path],
    delete: bool = False,
    exclude: Tuple[str, ...] = (),
    acl: Optional[str] = None,
    content_type: Optional[str] = None,
    content_encoding: Optional[str] = None,
    cache_control: Optional[str] = None,
):
    # must use sync for dirs and cp for files
    if isinstance(src, Path) and src.is_file():
        # must upload files with cp
        s3_cmd = "cp"
    else:
        s3_cmd = "sync"

    check_call(
        ["aws", "s3", s3_cmd, str(src), str(dst)]
        + (["--delete"] if delete else [])
        + [
            arg
            for key, value in zip(
                (
                    *("--exclude" for _ in exclude),
                    "--content-type",
                    "--content-encoding",
                    "--cache-control",
                    "--acl",
                ),
                (
                    *exclude,
                    content_type,
                    content_encoding,
                    cache_control,
                    acl,
                ),
            )
            if value is not None
            for arg in (key, value)
        ]
    )


def _gcs_sync(
    src: Union[str, Path],
    dst: Union[str, Path],
    delete: bool = False,
    exclude: Tuple[str, ...] = (),
    content_type: Optional[str] = None,
    content_encoding: Optional[str] = None,
    cache_control: Optional[str] = None,
    acl: Optional[str] = None,
):
    if isinstance(src, Path) and src.is_file():
        # must upload files with cp
        gsutil_cmd = ["cp"]
        if delete:
            raise ValueError("cannot delete when uploading a single file")
        if exclude:
            raise ValueError("cannot exclude when uploading a single file")
    else:
        gsutil_cmd = ["rsync", "-r"]

    check_call(
        ["gsutil", "-m"]
        # -h flags are global and must appear before the rsync/cp command
        + [
            arg
            for header, value in zip(
                ["Content-Type", "Content-Encoding", "Cache-Control"],
                [content_type, content_encoding, cache_control],
            )
            if value is not None
            for arg in ("-h", f"{header}:{value}")
        ]
        + gsutil_cmd
        # command specific options must appear before src and dst
        + (["-d"] if delete else [])
        # translate excludes from glob to regex before passing to gsutil
        + [arg for item in exclude for arg in ("-x", fnmatch.translate(item))]
        + (["-a", acl] if acl is not None else [])
        + [str(src), str(dst)]
    )


def _get_sync_function(remote: str):
    if remote.startswith("s3://"):
        return _s3_sync
    elif remote.startswith("gs://"):
        return _gcs_sync
    else:
        raise ValueError(
            f"remote path must have scheme like s3:// or gs://, got: {remote!r}"
        )


def remote_storage_pull(src: str, dst: Path, decompress: bool = False):
    if decompress:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            remote_storage_pull(src, tmp_path, decompress=False)
            for in_file in tmp_path.rglob("*"):
                if not in_file.is_dir():
                    out_file = dst / in_file.relative_to(tmp_path)
                    out_file.parent.mkdir(parents=True, exist_ok=True)
                    out_file.write_bytes(gzip.decompress(in_file.read_bytes()))
    else:
        _get_sync_function(src)(src, dst)


def remote_storage_push(src: Path, dst: str, compress: bool = False, **kwargs):
    sync = _get_sync_function(dst)
    if compress:
        kwargs["content_encoding"] = "gzip"
        if "exclude" in kwargs:
            raise NotImplementedError("exclude is not supported while compressing")
        # cloudfront is supposed to automatically gzip objects, but it won't do that
        # if the object size is > 10 megabytes (https://webmasters.stackexchange.com/a/111734)
        # which our files sometimes are. to work around this, as well as to support google
        # cloud storage, we'll gzip the contents into a temporary directory, and upload that
        # with a special content encoding
        with TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            if src.is_dir():
                for in_file in src.rglob("*"):
                    if not in_file.is_dir():
                        out_file = tmp / in_file.relative_to(src)
                        out_file.parent.mkdir(parents=True, exist_ok=True)
                        out_file.write_bytes(gzip.compress(in_file.read_bytes()))
                index = tmp / INDEX_HTML
                if index.exists():
                    # must be a tuple
                    kwargs["exclude"] = (INDEX_HTML,)
                sync(
                    src=tmp,
                    dst=dst,
                    content_type=APPLICATION_JSON,
                    **kwargs,
                )
                if index.exists():
                    # cannot delete or exclude with a single file
                    kwargs["delete"] = False
                    kwargs["exclude"] = ()
                    sync(
                        src=index,
                        dst=dst,
                        content_type=TEXT_HTML,
                        **kwargs,
                    )
            else:
                tmp_file = tmp / src.name
                tmp_file.write_bytes(gzip.compress(src.read_bytes()))
                content_type = TEXT_HTML if src.name == INDEX_HTML else APPLICATION_JSON
                sync(
                    src=tmp_file,
                    dst=dst,
                    content_type=content_type,
                    **kwargs,
                )
    else:
        sync(src, dst, **kwargs)
