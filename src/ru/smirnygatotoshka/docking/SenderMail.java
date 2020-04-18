package ru.smirnygatotoshka.docking;

import javax.mail.*;
import javax.mail.internet.*;
import java.util.Properties;

public class SenderMail {

    private String msg;

    public SenderMail(String msg) {
        this.msg = msg;
    }

    public void send() throws MessagingException {

        Properties prop = new Properties();
        prop.put("mail.smtp.auth", true);
        prop.put("mail.smtp.starttls.enable", "true");
        prop.put("mail.smtp.host", "smtp.gmail.com");
        prop.put("mail.smtp.port", "587");
        prop.put("mail.smtp.ssl.trust", "smtp.gmail.com");

        Session session = Session.getDefaultInstance(prop, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("rnimucluster@gmail.com","scezfeamnxluhovi");
            }
        });
        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress("rnimucluster@gmail.com"));
        message.setRecipients(
                Message.RecipientType.TO, InternetAddress.parse("anton.smirnov.9910@gmail.com"));
        message.setSubject("Cluster work");

        MimeBodyPart mimeBodyPart = new MimeBodyPart();
        mimeBodyPart.setContent(msg, "text/html");

        Multipart multipart = new MimeMultipart();
        multipart.addBodyPart(mimeBodyPart);
        message.setContent(multipart);
        message.saveChanges();

        Transport tr = session.getTransport("smtp");
        tr.connect(session.getProperty("mail.smtp.host"),  "rnimucluster@gmail.com","scezfeamnxluhovi");
        tr.sendMessage(message, message.getAllRecipients());
        tr.close();

        System.out.println("Mail was send successful!");
    }
}