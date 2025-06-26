package com.jpmc.midascore.listener;

import java.time.LocalDateTime;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;

@Component
public class KafkaTransactionListener {

    @Autowired
    private UserRepository userRepo;

    @Autowired
    private TransactionRecordRepository txRepo;

    private final RestTemplate restTemplate = new RestTemplate();

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(Transaction tx) {
        Optional<UserRecord> senderOpt = userRepo.findById(tx.getSenderId());
        Optional<UserRecord> recipientOpt = userRepo.findById(tx.getRecipientId());

        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) return;

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        if (sender.getBalance() < tx.getAmount()) return;

        // 💡 Call Incentive API
        float incentive = 0f;
        try {
            IncentiveResponse response = restTemplate.postForObject(
                "http://localhost:8080/incentive", tx, IncentiveResponse.class);
            if (response != null) {
                incentive = response.getAmount();
            }
        } catch (Exception e) {
            // If the incentive API fails, we skip incentive (fail silently)
            incentive = 0f;
        }

        // ✅ Update balances
        sender.setBalance(sender.getBalance() - tx.getAmount());
        recipient.setBalance(recipient.getBalance() + tx.getAmount() + incentive);

        userRepo.save(sender);
        userRepo.save(recipient);

        // ✅ Record transaction
        TransactionRecord record = new TransactionRecord();
        record.setSender(sender);
        record.setRecipient(recipient);
        record.setAmount(tx.getAmount());
        record.setIncentive(incentive);
        record.setTimestamp(LocalDateTime.now());

        txRepo.save(record);
    }

    // ✨ Inner class to map Incentive API response
    private static class IncentiveResponse {
        private float amount;

        public float getAmount() {
            return amount;
        }

        public void setAmount(float amount) {
            this.amount = amount;
        }
    }
}
