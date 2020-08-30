package com.prottonne.sqsclient.config.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.prottonne.sqsclient.dto.Request;
import com.prottonne.sqsclient.dto.Response;
import java.util.HashMap;
import java.util.Map;
import org.springframework.cloud.aws.messaging.config.annotation.EnableSqs;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.cloud.aws.messaging.core.SqsMessageHeaders;

@Service
@EnableSqs
public class Sender {

    private QueueMessagingTemplate queueMessagingTemplate;

    @Autowired
    public Sender(AmazonSQSAsync amazonSqs) {
        this.queueMessagingTemplate = new QueueMessagingTemplate(amazonSqs);
    }

    public Response send(Request request) {
        final String queueName = request.getQueueName();
        final String queueMessage = request.getQueueMessageJson();
        final String messageGroupId = request.getMessageGroupId();

        String sha256hex = org.apache.commons.codec.digest.DigestUtils.sha256Hex(queueMessage);
        Map<String, Object> headers = new HashMap<>();
        headers.put(SqsMessageHeaders.SQS_GROUP_ID_HEADER, messageGroupId);
        headers.put(SqsMessageHeaders.SQS_DEDUPLICATION_ID_HEADER, sha256hex);
        queueMessagingTemplate.convertAndSend(queueName, queueMessage, headers);

        return new Response();
    }

}
