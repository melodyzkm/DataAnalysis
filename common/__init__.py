def mail():
    my_sender = 'zhangzhangkm@126.com'  # 发件人邮箱账号
    my_pass = 'admin123'  # 发件人邮箱密码
    my_user = 'coolhandxs@126.com'  # 收件人邮箱账号，我这边发送给自己
    ret = True

    msg = MIMEMultipart()

    # 邮件正文是MIMEText:
    # msg.attach(MIMEText('send with file...', 'plain', 'utf-8'))
    msg = MIMEText('填写邮件内容', 'plain', 'utf-8')
    msg['From'] = formataddr(["From", my_sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
    msg['To'] = formataddr(["FK", my_user])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
    msg['Subject'] = "MBT日报"  # 邮件的主题，也可以说是标题

    # 添加附件就是加上一个MIMEBase，从本地读取一个图片:
    with open('daily_report.csv', 'rt') as f:
        # 设置附件的MIME和文件名，这里是png类型:
        mime = MIMEBase('document', 'csv', filename='daily_report.csv')
        # 加上必要的头信息:
        mime.add_header('Content-Disposition', 'attachment', filename='daily_report.csv')
        mime.add_header('Content-ID', '<0>')
        mime.add_header('X-Attachment-Id', '0')
        # 把附件的内容读进来:
        mime.set_payload(f.read())
        # 用Base64编码:
        # encoders.encode_base64(mime)
        # 添加到MIMEMultipart:
        msg.attach(mime)

    server = smtplib.SMTP_SSL("smtp.126.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
    server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
    server.sendmail(my_sender, [my_user, ], msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
    server.quit()  # 关闭连接