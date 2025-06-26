package com.jpmc.midascore.repository;

import java.util.Optional;

import org.springframework.data.repository.CrudRepository;

import com.jpmc.midascore.entity.UserRecord;


public interface UserRepository extends CrudRepository<UserRecord, Long> {
    Optional<UserRecord> findById(Long id);
    Optional<UserRecord> findByName(String name); 
}
