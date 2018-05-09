# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import boto3

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_ses(fromaddr,
             subject,
             body,
             recipients,
             filename='',
             dryrun=False):
    """Send an email via the Amazon SES service. Can specify a single or list of
       recipients.

    Examples:
    ```
    send_ses('me@example.com', 'greetings', "Hi!", 'you@example.com')
    ```

    ```
    send_ses('me@example.com', 'greetings', "Hi!", ['a@example.com`, 'b@example.com'])
    ```

    Raises a RuntimeError if the message did not send correctly."""

    if isinstance(recipients, list):
        recipients = ','.join(recipients)

    if dryrun:
        print '\n'.join([
            "Would send email",
            "From: " + fromaddr,
            "To:" + recipients,
            "Subject: " + subject,
            "Body: " + body])

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = recipients
    msg.attach(MIMEText(body))

    if filename:
        attachment = open(filename, 'rb').read()
        part = MIMEApplication(attachment)
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)

    ses = boto3.client('ses', region_name='us-west-2')
    result = ses.send_raw_email(
        RawMessage={'Data': msg.as_string()}
    )

    if 'ErrorResponse' in result:
        raise RuntimeError("Error sending email: " + result)
