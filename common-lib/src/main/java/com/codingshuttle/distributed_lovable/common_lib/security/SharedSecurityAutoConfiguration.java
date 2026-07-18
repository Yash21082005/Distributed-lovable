package com.codingshuttle.distributed_lovable.common_lib.security;


import feign.RequestInterceptor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.servlet.HandlerExceptionResolver;


@AutoConfiguration
public class SharedSecurityAutoConfiguration {

    @Bean
    public AuthUtil authUtil() {
        return new AuthUtil();
    }

    @Bean
    public JwtAuthFilter jwtAuthFilter(AuthUtil authUtil, @Qualifier("handlerExceptionResolver")
                                        HandlerExceptionResolver handlerExceptionResolver) {
        return new JwtAuthFilter(authUtil, handlerExceptionResolver);
    }

    @Bean
    public RequestInterceptor requestInterceptor() {
        return requestTemplate -> {
            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

            System.out.println("=================================");
            System.out.println("Feign Authentication = " + authentication);

            if (authentication != null) {
                System.out.println("Principal = " + authentication.getPrincipal());
                System.out.println("Credentials = " + authentication.getCredentials());
            }

            String token = null;

            if (authentication != null && authentication.getCredentials() instanceof String credentialsToken) {
                token = credentialsToken;
            } else {
                token = FeignAuthTokenContext.get();
                System.out.println("SecurityContext empty, falling back to FeignAuthTokenContext = " + token);
            }

            if (token != null) {
                System.out.println("JWT Forwarded");
                requestTemplate.header("Authorization", "Bearer " + token);
            }
        };
    }

//    @Bean
//    public RequestInterceptor requestInterceptor() {
//        return requestTemplate -> {
//            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
//
//            if (authentication != null && authentication.getCredentials() instanceof String token) {
//                requestTemplate.header("Authorization", "Bearer " + token);
//            }
//        };
//    }
}
