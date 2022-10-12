package com.learn.kafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
public class FailureRecord {

    @Id
    @GeneratedValue
    private Integer id;
    private String topic;
    private Integer messageKey;
    private String errorRecord;
    private Integer partition;
    private Long offsetValue;
    @Column(name = "EXCEPTION", columnDefinition = "TEXT")
    private String exception;
    private String status;
}
