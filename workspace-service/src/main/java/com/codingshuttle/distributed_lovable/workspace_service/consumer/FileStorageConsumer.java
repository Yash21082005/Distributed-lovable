package com.codingshuttle.distributed_lovable.workspace_service.consumer;

import com.codingshuttle.distributed_lovable.common_lib.event.FileStoreRequestEvent;
import com.codingshuttle.distributed_lovable.workspace_service.service.ProjectFileService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
@Slf4j
@RequiredArgsConstructor
public class FileStorageConsumer {

    private final ProjectFileService projectFileService;

    @Transactional
    @KafkaListener(topics = "file-storage-request-event", groupId = "workspace-group")
    public void consumeFileEvent(FileStoreRequestEvent requestEvent) {


        log.info("Saving file: {}", requestEvent.filePath());

        projectFileService.saveFile(requestEvent.projectId(), requestEvent.filePath(), requestEvent.content());

    }
}