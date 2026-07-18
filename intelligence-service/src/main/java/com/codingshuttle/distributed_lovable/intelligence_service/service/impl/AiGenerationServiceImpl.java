package com.codingshuttle.distributed_lovable.intelligence_service.service.impl;

import com.codingshuttle.distributed_lovable.common_lib.enums.ChatEventType;
import com.codingshuttle.distributed_lovable.common_lib.enums.MessageRole;
import com.codingshuttle.distributed_lovable.common_lib.error.ResourceNotFoundException;
import com.codingshuttle.distributed_lovable.common_lib.event.FileStoreRequestEvent;
import com.codingshuttle.distributed_lovable.common_lib.security.AuthUtil;
import com.codingshuttle.distributed_lovable.intelligence_service.client.WorkspaceClient;
import com.codingshuttle.distributed_lovable.intelligence_service.dto.chat.StreamResponse;
import com.codingshuttle.distributed_lovable.intelligence_service.entity.ChatEvent;
import com.codingshuttle.distributed_lovable.intelligence_service.entity.ChatMessage;
import com.codingshuttle.distributed_lovable.intelligence_service.entity.ChatSession;
import com.codingshuttle.distributed_lovable.intelligence_service.entity.ChatSessionId;
import com.codingshuttle.distributed_lovable.intelligence_service.llm.CodeGenerationTools;
import com.codingshuttle.distributed_lovable.intelligence_service.llm.FileTreeContextAdvisor;
import com.codingshuttle.distributed_lovable.intelligence_service.llm.LlmResponseParser;
import com.codingshuttle.distributed_lovable.intelligence_service.llm.PromptUtils;
import com.codingshuttle.distributed_lovable.intelligence_service.repository.ChatEventRepository;
import com.codingshuttle.distributed_lovable.intelligence_service.repository.ChatMessageRepository;
import com.codingshuttle.distributed_lovable.intelligence_service.repository.ChatSessionRepository;
import com.codingshuttle.distributed_lovable.intelligence_service.service.AiGenerationService;
import com.codingshuttle.distributed_lovable.intelligence_service.service.UsageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
@Slf4j
public class AiGenerationServiceImpl implements AiGenerationService {

    private final ChatClient chatClient;
    private final AuthUtil authUtil;
    private final FileTreeContextAdvisor fileTreeContextAdvisor;
    private final ChatSessionRepository chatSessionRepository;
    private final LlmResponseParser llmResponseParser;
    private final ChatMessageRepository chatMessageRepository;
    private final ChatEventRepository chatEventRepository;
    private final UsageService usageService;
    private final WorkspaceClient workspaceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;


    @Override
    @PreAuthorize("@security.canEditProject(#projectId)")
    public Flux<StreamResponse> streamResponse(String userMessage, Long projectId) {

//        usageService.checkDailyTokensUsage();

        Long userId = authUtil.getCurrentUserId();
        ChatSession chatSession = createChatSessionIfNotExists(projectId, userId);
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String authToken = (authentication != null && authentication.getCredentials() instanceof String token)
                ? token
                : null;

        Map<String,Object> advisorParams = new HashMap<>();
        advisorParams.put("userId", userId);
        advisorParams.put("projectId", projectId);
        advisorParams.put("authToken", authToken);



        StringBuilder fullResponseBuffer = new StringBuilder();
        CodeGenerationTools codeGenerationTools = new CodeGenerationTools(projectId, workspaceClient,authToken);

        AtomicReference<Long> startTime = new AtomicReference<>(System.currentTimeMillis());
        AtomicReference<Long> endTime = new AtomicReference<>(0L);
        AtomicReference<Usage> usageRef = new AtomicReference<>();

        return chatClient.prompt()
                .system(PromptUtils.CODE_GENERATION_SYSTEM_PROMPT)
                .user(userMessage)
                .tools(codeGenerationTools)
                .advisors(advisorSpec -> {
                            advisorSpec.params(advisorParams);
                            advisorSpec.advisors(fileTreeContextAdvisor);
                        }
                )
                .stream()
                .chatResponse()
                .doOnNext(response -> {
                    String content = response.getResult().getOutput().getText();

                    if(content != null && !content.isEmpty() && endTime.get() == 0) { // first non-empty chunk received
                        endTime.set(System.currentTimeMillis());
                    }

                    if(response.getMetadata().getUsage() != null) {
                        usageRef.set(response.getMetadata().getUsage());
                    }

                    fullResponseBuffer.append(content);
                })
                .doOnComplete(() -> {
                    Schedulers.boundedElastic().schedule(() -> {
//                        parseAndSaveFiles(fullResponseBuffer.toString(), projectId);

                        long duration = (endTime.get() - startTime.get()) /  1000;
                        finalizeChats(userMessage, chatSession, fullResponseBuffer.toString(), duration, usageRef.get(), userId);
                    });
                })
                .doOnError(error -> log.error("Error during streaming for projectId: {}", projectId))
                .map(response -> {
                    String text = response.getResult().getOutput().getText();
                    return new StreamResponse(text != null ? text : "");
                });
    }

    private void finalizeChats(String userMessage, ChatSession chatSession, String fullText, Long duration, Usage usage, Long userId) {
        Long projectId = chatSession.getId().getProjectId();

        if(usage != null) {
            int totalTokens = usage.getTotalTokens();
            usageService.recordTokenUsage(chatSession.getId().getUserId(), totalTokens);
        }

        // Save the User message
        chatMessageRepository.save(
                ChatMessage.builder()
                        .chatSession(chatSession)
                        .role(MessageRole.USER)
                        .content(userMessage)
                        .tokensUsed(usage.getPromptTokens())
                        .build()
        );

        ChatMessage assistantChatMessage = ChatMessage.builder()
                .role(MessageRole.ASSISTANT)
                .content("Assistant Message here...")
                .chatSession(chatSession)
                .tokensUsed(usage.getCompletionTokens())
                .build();

        assistantChatMessage = chatMessageRepository.save(assistantChatMessage);

        List<ChatEvent> chatEventList = llmResponseParser.parseChatEvents(fullText, assistantChatMessage);
        chatEventList.addFirst(ChatEvent.builder()
                .type(ChatEventType.THOUGHT)
             //   .status(ChatEventStatus.CONFIRMED)
                .chatMessage(assistantChatMessage)
                .content("Thought for "+duration+"s")
                .sequenceOrder(0)
                .build());

        chatEventList.stream()
                .filter(e -> e.getType() == ChatEventType.FILE_EDIT)
                .forEach(e -> {
//                    String sagaId = UUID.randomUUID().toString();
//                    e.setSagaId(sagaId);
                    FileStoreRequestEvent fileStoreRequestEvent = new FileStoreRequestEvent(
                            projectId,
                            "",
                            e.getFilePath(),
                            e.getContent(),
                            userId
                    );
                    log.info("Storage request event sent: {}", e.getFilePath());
                    kafkaTemplate.send("file-storage-request-event", "project-"+projectId, fileStoreRequestEvent);
                });

        chatEventRepository.saveAll(chatEventList);
    }

    private ChatSession createChatSessionIfNotExists(Long projectId, Long userId) {
        ChatSessionId chatSessionId = new ChatSessionId(projectId, userId);
        ChatSession chatSession = chatSessionRepository.findById(chatSessionId).orElse(null);

        if(chatSession == null) {
            chatSession = ChatSession.builder()
                    .id(chatSessionId)
                    .build();

            chatSession = chatSessionRepository.save(chatSession);
        }
        return chatSession;
    }
}




//@RequiredArgsConstructor
//@Slf4j
//@Service
//public class AiGenerationServiceImpl implements AiGenerationService {
//
//    private final ChatClient chatClient;
//    private final AuthUtil authUtil;
//    private static final Pattern FILE_TAG_PATTERN = Pattern.compile("<file path=\"([^\"]+)\">(.*?)</file>",Pattern.DOTALL);
//    private final FileTreeContextAdvisor fileTreeContextAdvisor;
//    private final LlmResponseParser llmResponseParser;
//    private final ChatSessionRepository chatSessionRepository;
//    private final ChatMessageRepository chatMessageRepository;
//    private final ChatEventRepository chatEventRepository;
//    private final UsageService usageService;
//    private final WorkspaceClient workspaceClient;
//    private final KafkaTemplate<String, Object> kafkaTemplate;
//
//    @Override
//    @PreAuthorize("@security.canEditProject(#projectId)")
//    public Flux<StreamResponse> streamResponse(String userMessage, Long projectId) {
//
////        usageService.checkDailyTokensUsage();
//
//        Long userId=authUtil.getCurrentUserId();
//
//        ChatSession chatSession =  createChatSessionIfNotExists(projectId,userId);
//
//        // Captured here, on the synchronous request thread, while SecurityContextHolder is
//        // still guaranteed to be populated by JwtAuthFilter. FileTreeContextAdvisor and
//        // CodeGenerationTools run inside the reactive ChatClient stream/tool-calling pipeline,
//        // where the ThreadLocal SecurityContext is no longer reliably available, so the raw
//        // token is threaded through explicitly instead.
//        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
//        String authToken = (authentication != null && authentication.getCredentials() instanceof String token)
//                ? token
//                : null;
//
//        Map<String,Object> advisorParams = new HashMap<>();
//        advisorParams.put("userId", userId);
//        advisorParams.put("projectId", projectId);
//        advisorParams.put("authToken", authToken);
//
//        StringBuilder fullResponseBuffer=new StringBuilder();
//
//        CodeGenerationTools codeGenerationTools=new CodeGenerationTools(projectId,workspaceClient,authToken);
//
//        AtomicReference<Long> startTime =new AtomicReference<>(System.currentTimeMillis());
//        AtomicReference<Long> endTime=new AtomicReference<>(0L);
//        AtomicReference<Usage> usageRef = new AtomicReference<>();
//
//        System.out.println("Before ChatClient = " + authentication);
//
//        return chatClient.prompt()
//                .system(PromptUtils.CODE_GENERATION_SYSTEM_PROMPT)
//                .user(userMessage)
//                .tools(codeGenerationTools)
//                .advisors(advisorSpec -> {
//                    advisorSpec.params(advisorParams);
//                    advisorSpec.advisors(fileTreeContextAdvisor);
//                }).stream()
//                .chatResponse()
//
//
////        return chatClient.prompt()
////                .system("You are a helpful assistant.")
////                .user("Say hello")
////                .stream()
////                .chatResponse()
//                .doOnNext(response->{
//
////                    String content=response.getResult().getOutput().getText();
//
//                    String content = "";
//
//                    if (response.getResult() != null
//                            && response.getResult().getOutput() != null
//                            && response.getResult().getOutput().getText() != null) {
//
//                        content = response.getResult().getOutput().getText();
//                    }
//
//                    if(content!=null && !content.isEmpty() && endTime.get() == 0){
//                        endTime.set(System.currentTimeMillis());
//                    }
//
//                    if(response.getMetadata().getUsage() != null) {
//                        usageRef.set(response.getMetadata().getUsage());
//                    }
//                    fullResponseBuffer.append(content);
//                }).
////                .doOnNext(response -> {
////
////
////                    if(response.getResult() != null &&
////                            response.getResult().getOutput() != null &&
////                            response.getResult().getOutput().getText() != null) {
////
////                        String content = response.getResult().getOutput().getText();
////                        fullResponseBuffer.append(content);
////                    }
////                }).
//        doOnComplete(()->{
//    Schedulers.boundedElastic().schedule(()->{
//    //    parseAndSaveFiles(fullResponseBuffer.toString(), projectId);
//
//        long duration=(endTime.get() - startTime.get())/1000;
//        finalizeChats(userMessage,chatSession, fullResponseBuffer.toString(),duration,usageRef.get(), userId);
//    });
//    System.out.println("=====================================");
//    System.out.println(fullResponseBuffer.toString());
//    System.out.println("=====================================");
//}).doOnError(error->log.error("Error during streaming for projectId: {}", projectId))
//
////                .map(response-> Objects.requireNonNull(response.getResult().getOutput().getText()));
////                .map(response -> {
////                    if(response.getResult() == null ||
////                            response.getResult().getOutput() == null ||
////                            response.getResult().getOutput().getText() == null) {
////                        return "";
////                    }
////
////                    return response.getResult().getOutput().getText();
////                })
////                .filter(text -> !text.isBlank());
//
////                .map(response -> {
////                    String text = response.getResult().getOutput().getText();
////                    return new StreamResponse(text != null ? text : "");
////                });
//
//                .map(response -> {
//
//                    if (response.getResult() == null
//                            || response.getResult().getOutput() == null
//                            || response.getResult().getOutput().getText() == null) {
//                        return new StreamResponse("");
//                    }
//
//                    return new StreamResponse(
//                            response.getResult().getOutput().getText()
//                    );
//                });
//    }
//
////    private void parseAndSaveFiles(String fullResponse, Long projectId) {
////
////        Matcher matcher=FILE_TAG_PATTERN.matcher(fullResponse);
////        while(matcher.find()){
////            String filePath=matcher.group(1);
////            String fileContent=matcher.group(2).trim();
////            projectFileService.saveFile(projectId,filePath,fileContent);
////        }
////    }
//
//    private void finalizeChats(String userMessage, ChatSession chatSession,String fullText,Long duration, Usage usage, Long userId){
//
//        Long projectId=chatSession.getId().getProjectId();
//
//
//        if(usage != null) {
//            int totalTokens = usage.getTotalTokens();
//            usageService.recordTokenUsage(chatSession.getId().getUserId(), totalTokens);
//        }
//
//        // Save the User message
//        chatMessageRepository.save(
//                ChatMessage.builder()
//                        .chatSession(chatSession)
//                        .role(MessageRole.USER)
//                        .content(userMessage)
////                        .tokensUsed(usage.getPromptTokens())
//                        .tokensUsed(usage != null ? usage.getPromptTokens() : 0)
//                        .build()
//        );
//
//        ChatMessage assistantChatMessage = ChatMessage.builder()
//                .role(MessageRole.ASSISTANT)
//                .content("Assistant Message here...")
//                .chatSession(chatSession)
////                .tokensUsed(usage.getCompletionTokens())
//                .tokensUsed(usage != null ? usage.getCompletionTokens() : 0)
//                .build();
//
//        assistantChatMessage=chatMessageRepository.save(assistantChatMessage);
//
//        List<ChatEvent> chatEventList = llmResponseParser.parseChatEvents(fullText, assistantChatMessage);
//        chatEventList.addFirst(ChatEvent.builder()
//                .type(ChatEventType.THOUGHT)
//                .chatMessage(assistantChatMessage)
//                .content("Thought for "+duration+"s")
//                .sequenceOrder(0)
//                .build());
//        chatEventList.stream()
//                .filter(e->e.getType() == ChatEventType.FILE_EDIT)
//                .forEach(e->{
//                    FileStoreRequestEvent fileStoreRequestEvent = new FileStoreRequestEvent(
//                            projectId, "",
//                            e.getFilePath(),
//                            e.getContent(),
//                            userId
//                    );
//                    log.info("Storage request event sent : {}", e.getFilePath());
//                    kafkaTemplate.send("file-storage-request-event","project-"+projectId,fileStoreRequestEvent);
//                });
//
//        chatEventRepository.saveAll(chatEventList);
//    }
//
//
//
//    private ChatSession createChatSessionIfNotExists(Long projectId, Long userId) {
//
//        ChatSessionId chatSessionId=new ChatSessionId(projectId,userId);
//        ChatSession chatSession = chatSessionRepository.findById(chatSessionId).orElse(null);
//
//        if(chatSession == null){
//            chatSession=ChatSession.builder()
//                    .id(chatSessionId)
//                    .build();
//
//            chatSession =  chatSessionRepository.save(chatSession);
//        }
//        return chatSession;
//    }
//}






//@RequiredArgsConstructor
//@Slf4j
//@Service
//public class AiGenerationServiceImpl implements AiGenerationService {
//
//    private final ChatClient chatClient;
//    private final AuthUtil authUtil;
//    private static final Pattern FILE_TAG_PATTERN = Pattern.compile("<file path=\"([^\"]+)\">(.*?)</file>",Pattern.DOTALL);
//    private final ProjectFileService projectFileService;
//    private final FileTreeContextAdvisor fileTreeContextAdvisor;
//    private final LlmResponseParser llmResponseParser;
//    private final ChatSessionRepository chatSessionRepository;
//    private final ProjectRepository projectRepository;
//    private final UserRepository userRepository;
//    private final ChatMessageRepository chatMessageRepository;
//    private final ChatEventRepository chatEventRepository;
//    private final UsageService usageService;
//
//    @Override
//    @PreAuthorize("@security.canEditProject(#projectId)")
//    public Flux<StreamResponse> streamResponse(String userMessage, Long projectId) {
//
////        usageService.checkDailyTokensUsage();
//
//        Long userId=authUtil.getCurrentUserId();
//
//       ChatSession chatSession =  createChatSessionIfNotExists(projectId,userId);
//
//        Map<String,Object> advisorParams=Map.of(
//                "userId", userId,
//                "projectId", projectId
//        );
//
//        StringBuilder fullResponseBuffer=new StringBuilder();
//
//        CodeGenerationTools codeGenerationTools=new CodeGenerationTools(projectFileService,projectId);
//
//        AtomicReference<Long> startTime =new AtomicReference<>(System.currentTimeMillis());
//        AtomicReference<Long> endTime=new AtomicReference<>(0L);
//        AtomicReference<Usage> usageRef = new AtomicReference<>();
//
//
//        return chatClient.prompt()
//                .system(PromptUtils.CODE_GENERATION_SYSTEM_PROMPT)
//                .user(userMessage)
//                .tools(codeGenerationTools)
//                .advisors(advisorSpec -> {
//                    advisorSpec.params(advisorParams);
//                    advisorSpec.advisors(fileTreeContextAdvisor);
//                }).stream()
//                .chatResponse()
//                .doOnNext(response->{
//
//                    String content=response.getResult().getOutput().getText();
//
//                    if(content!=null && !content.isEmpty() && endTime.get() == 0){
//                        endTime.set(System.currentTimeMillis());
//                    }
//
//                    if(response.getMetadata().getUsage() != null) {
//                        usageRef.set(response.getMetadata().getUsage());
//                    }
//                    fullResponseBuffer.append(content);
//                }).
////                .doOnNext(response -> {
////
////
////                    if(response.getResult() != null &&
////                            response.getResult().getOutput() != null &&
////                            response.getResult().getOutput().getText() != null) {
////
////                        String content = response.getResult().getOutput().getText();
////                        fullResponseBuffer.append(content);
////                    }
////                }).
//                doOnComplete(()->{
//                    Schedulers.boundedElastic().schedule(()->{
//                        parseAndSaveFiles(fullResponseBuffer.toString(), projectId);
//
//                        long duration=(endTime.get() - startTime.get())/1000;
//                        finalizeChats(userMessage,chatSession, fullResponseBuffer.toString(),duration,usageRef.get(), userId);
//                    });
//                }).doOnError(error->log.error("Error during streaming for projectId: {}", projectId))
//
////                .map(response-> Objects.requireNonNull(response.getResult().getOutput().getText()));
////                .map(response -> {
////                    if(response.getResult() == null ||
////                            response.getResult().getOutput() == null ||
////                            response.getResult().getOutput().getText() == null) {
////                        return "";
////                    }
////
////                    return response.getResult().getOutput().getText();
////                })
////                .filter(text -> !text.isBlank());
//
//                .map(response -> {
//                    String text = response.getResult().getOutput().getText();
//                    return new StreamResponse(text != null ? text : "");
//                });
//    }
//
//    private void parseAndSaveFiles(String fullResponse, Long projectId) {
//
//        Matcher matcher=FILE_TAG_PATTERN.matcher(fullResponse);
//        while(matcher.find()){
//            String filePath=matcher.group(1);
//            String fileContent=matcher.group(2).trim();
//            projectFileService.saveFile(projectId,filePath,fileContent);
//        }
//    }
//
//    private void finalizeChats(String userMessage, ChatSession chatSession,String fullText,Long duration, Usage usage, Long userId){
//
//        Long projectId=chatSession.getProject().getId();
//
//
//        if(usage != null) {
//            int totalTokens = usage.getTotalTokens();
//            usageService.recordTokenUsage(userId, totalTokens);
//        }
//
//        // Save the User message
//        chatMessageRepository.save(
//                ChatMessage.builder()
//                        .chatSession(chatSession)
//                        .role(MessageRole.USER)
//                        .content(userMessage)
//                        .tokensUsed(usage.getPromptTokens())
//                        .build()
//        );
//
//        ChatMessage assistantChatMessage = ChatMessage.builder()
//                .role(MessageRole.ASSISTANT)
//                .content("Assistant Message here...")
//                .chatSession(chatSession)
//                .tokensUsed(usage.getCompletionTokens())
//                .build();
//
//        assistantChatMessage=chatMessageRepository.save(assistantChatMessage);
//
//        List<ChatEvent> chatEventList = llmResponseParser.parseChatEvents(fullText, assistantChatMessage);
//        chatEventList.addFirst(ChatEvent.builder()
//                        .type(ChatEventType.THOUGHT)
//                        .chatMessage(assistantChatMessage)
//                        .content("Thought for "+duration+"s")
//                        .sequenceOrder(0)
//                .build());
//        chatEventList.stream()
//                .filter(e->e.getType() == ChatEventType.FILE_EDIT)
//                .forEach(e->projectFileService.saveFile(projectId,e.getFilePath(), e.getContent()));
//
//        chatEventRepository.saveAll(chatEventList);
//    }
//
//
//
//    private ChatSession createChatSessionIfNotExists(Long projectId, Long userId) {
//
//        ChatSessionId chatSessionId=new ChatSessionId(projectId,userId);
//        ChatSession chatSession = chatSessionRepository.findById(chatSessionId).orElse(null);
//
//        if(chatSession == null){
//            Project project=   projectRepository.findById(projectId)
//                    .orElseThrow(()-> new ResourceNotFoundException("Project", projectId.toString()));
//
//            User user=userRepository.findById(userId).orElseThrow(()-> new ResourceNotFoundException("User", userId.toString()));;
//            chatSession=ChatSession.builder()
//                    .id(chatSessionId)
//                    .project(project)
//                    .user(user)
//                    .build();
//
//         chatSession =  chatSessionRepository.save(chatSession);
//        }
//        return chatSession;
//    }
//}
