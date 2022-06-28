---
layout: post
title: "Send a spark dataframe over email from databricks"
subtitle: "And style your dataframe with custom CSS!"
date: 2022-06-27 11:11:11 -0400
background: "/img/posts/02.jpg"
---

## TL;DR

Download the below notebook [here](../../../notebooks/2022-06-27-email_from_pyspark.ipynb) with comments from the blog.

OR

View this notebook on github [here](https://github.com/mar1boroman/databricks-patterns/blob/main/notebooks/2022-06-27-email_from_pyspark.ipynb)

This notebook has been tested on Databricks Community Edition

## Why send emails from your pyspark notebook ?

There are a few (but notable) use cases where you would want to send emails from your databricks/pyspark notebooks.

A straightforward use case is for operations teams who need to monitor their notebooks/spark job runs.
A reusable email function can come in handy, you could just plug it into any notebook and invoke the function wherever there is an error and email an error message notification
Although we could argue that mordern cloud systems may give similar notification functionality, if you want granular control on your emailing system, its best to use a custom emailing function.

Similarly, you could want a notification whenever your spark notebook run is completed.

## Emailing spark dataframes ?

Another interesting use case would be for aggregated operational reporting.

_Say you need to send a simple tabular report to your customers every hour, based on a complex view based on your underlying delta/parquet tables in your data-lake.
One way could definetly be to leverage any reporting tool at your disposal._

**Or else, you could create the dataframe in spark and send it across as an HTML email!**

## Here is a sample email sent using the below functions

![sample spark dataframe email](../../../img/posts/2022-06-27-email_from_pyspark/2022-06-27-email_from_pyspark.png)

## Assumptions & Warnings

The below code is suitable for small to medium dataframes. More than a performance issue, I couldnt imagine sending a dataframe with a million records in an email.
I would be writing about sending email attachments in an seperate post

## Let's Begin

Since I was using databricks community edition which comes with a pre-loaded dataset, we would run the below code to quickly build a dataframe.
**send_df** would be the dataframe we send through an HTML email

```python
send_df = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
send_df.write.format("delta").mode("overwrite").save("/delta/diamonds")
```

<br>

## A sample custom CSS string used for styling the dataframe

If you have ever used CSS, you would recognize the string defined below as pure CSS code without any modifications.
We would use this to style our dataframe (which gets converted to an HTML table)

Please note that not all CSS works in an email.
To check the latest supported CSS please refer [Gmail Supported CSS](https://developers.google.com/gmail/design/reference/supported_css)

```python
custom_table_style = '''
*{
    box-sizing: border-box;
    -webkit-box-sizing: border-box;
    -moz-box-sizing: border-box;
}
body{
    font-family: Helvetica;
    -webkit-font-smoothing: antialiased;
}

.table_header {
    text-align: center;
    background-color: #efefef;
    padding: 10px;
    margin: 0px 70px 0px 70px;
    border-top-left-radius: 15px;
    border-top-right-radius: 15px;
}

.table_header p {
    color: #9a8c98;
    font-weight: light;
}

.table_wrapper {
    margin: 0px 70px 10px 70px;
}

.custom_table {
    border-radius: 5px;
    font-size: 12px;
    font-weight: normal;
    border: thin solid #f2e9e4;
    border-collapse: collapse;
    width: 100%;
    max-width: 100%;
    white-space: nowrap;
    background-color: #f2e9e4;
    word-break: break-all;
    word-wrap: break-word;
}

.custom_table td, .custom_table th {
    text-align: center;
    padding: 8px;
}

.custom_table td {
    font-size: 12px;
}

.custom_table thead th {
    color: #edede9;
    background: #22223b;
}
'''
```

<br>

## Functions for building and sending emails

### build_html_email_body

This function is used to build your HTML message in case you wish to define a template email using your dataframes.
We design the function to accept multiple dataframes & also some flexibility to add pre and post html for Salutations and Signatures.

Please make sure you use the **max_rows_per_df** parameter to limit the number of records fetched from the dataframe
You can change this function according to your requirement. The parameters are explained in the docstring.

### sendmail_df

This function sends the HTML email.
If you wish to use this function simply to send an email, you could.
If you wish to send a dataframe in the email, use this function with the build_html_body_email function.
The parameters are explained in the docstring.

```python
def build_html_email_body(dataframes_list, pre_html_body = '', post_html_body = '', custom_css='', custom_css_class=None, max_rows_per_df=10):

    '''
    Author : Omkar Konnur
    License : MIT

    This function helps to compose an HTML email from your dataframes. Offers a few necessary customizations. Can be extended per your requirement.

    dataframes_list     : list of dataframes to be sent in the email. for e.g [df1,df2,...]
    pre_html_body       : Any html to be appended before the dataframe is displayed in the email. For e.g. '<p>Hi,</p>'
    post_html_body      : Any html to be appended in the end of the email. For e.g. Signatures, closing comments, etc.
    custom_css          : To format the table. Simply, this is the content of your CSS file. Note that the next parameter should pass the class defined in this CSS file.
    custom_css_class    : Single class used to modify the table CSS. This can be done as shown in the doc above
    max_rows_per_df     : Number of records in the dataframe sent in the email. Defaults to 10

    Please note that not all CSS works in an email. To check the latest supported CSS please refer - https://developers.google.com/gmail/design/reference/supported_css
    '''

    html_sep = '<br>'
    html_body = '<html><head><style>' + custom_css +'</style></head><body>' + pre_html_body
    for df in dataframes_list:
        df_count = df.count()
        html_body += f'''
                        <div class = 'table_header'>
                            <h3>Dataframe Total Count : {df_count}</h3>
                            <p> SHOWING MAX {max_rows_per_df} RECORDS FROM THE DATAFRAME </p>
                        </div>
                     '''
        html_body += f'''
                        <div class='table_wrapper'>
                            {df.limit(max_rows_per_df).toPandas().to_html(classes=custom_css_class)}
                        </div>
                     ''' + html_sep

    html_body+=post_html_body+'</body></html>'

    return html_body


def sendmail_html(smtp_server, smtp_port, smtp_user, smtp_password, sender_email, receiver_email, email_subject, email_body):
    import smtplib, ssl
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from datetime import datetime

    '''
    Author : Omkar Konnur
    License : MIT

    This function sends email from your python environment. Accepts message type as HTML.

    Usually the SMTP server details will be shared by your organization.
    For testing, you can use your gmail account or use free email services like SendGrid. (https://sendgrid.com)

    smtp_server        : SMTP Server (for e.g smtp.sendgrid.net)
    smtp_port          : SMTP Port
    smtp_user          : SMTP User
    smtp_password      : SMTP User

    sender_email       : Sender's email. Please verify the domains allowed by your SMTP server
    receiver_email     : Receiver's email. In case of multiple recipients, provide a semi-colon seperated string with different emails
    email_subject      : Subject Line. This function has been configured to pre-pend your Subject line with Timestamp and Cluster Name
    email_body         : HTML string
    '''

    email_subject = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {spark.conf.get('spark.databricks.clusterUsageTags.clusterName')} | {email_subject}"

    email_message = MIMEMultipart()
    email_message['From'] = sender_email
    email_message['To'] = receiver_email
    email_message['Subject'] = email_subject

    email_message.attach(MIMEText(email_body, "html"))
    email_string = email_message.as_string()

    with smtplib.SMTP_SSL(smtp_server, smtp_port, context=ssl.create_default_context()) as server:
        server.login(smtp_user, smtp_password)
        server.sendmail(sender_email, receiver_email, email_string)
```

<br>

## Testing and Execution

Make sure you get the below details before you attempt to send emails from your cluster.

```python
smtp_server = 'Your SMTP Server' # for e.g. 'smtp.sendgrid.net'
smtp_port = YourSMTPPort # for e.g 465
smtp_user = 'Enter your smtp username'
smtp_password = 'Enter your smtp password here'
email_sender = 'sender@domain.com'
email_receiver = 'receiver@domain.com'
```

And finally, the execution-

```python
email_body = build_html_email_body([send_df,send_df],custom_css=custom_table_style, custom_css_class='custom_table',max_rows_per_df=20)

sendmail_html(smtp_server, smtp_port, smtp_user, smtp_password,
            email_sender, email_receiver, 'My Awesome Dataframe with Custom Styling!',
            email_body
           )
```

<br>

## Conclusion

This is a simple and straightforward way to send emails from your spark cluster.
You could extend and customize the **build_html_email_body** function per your project requirement.
There are multiple features and filters available in the **to_html()** function in Pandas DataFrame class.

Happy emailing!
