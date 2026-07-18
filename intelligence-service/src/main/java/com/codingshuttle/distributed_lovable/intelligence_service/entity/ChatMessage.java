package com.codingshuttle.distributed_lovable.intelligence_service.entity;

import com.codingshuttle.distributed_lovable.common_lib.enums.MessageRole;
import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.List;

@Entity
@Table(name="chat_messages")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@FieldDefaults(level= AccessLevel.PRIVATE)
public class ChatMessage {

    @Id
            @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
            @JoinColumns({
                    @JoinColumn(name = "project_id", referencedColumnName = "projectId", nullable = false),
                    @JoinColumn(name = "user_id", referencedColumnName = "userId", nullable = false)
            })
    ChatSession chatSession;

    @Column(columnDefinition = "text")
    String content; // NULL unless user role


    @Enumerated(EnumType.STRING)
            @Column(nullable = false)
    MessageRole role;

    @OneToMany(mappedBy = "chatMessage", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @OrderBy("sequenceOrder ASC")
    List<ChatEvent> events;  // empty unless ASSISTANT role

    Integer tokensUsed=0;

    @CreationTimestamp
    Instant createdAt;
}
