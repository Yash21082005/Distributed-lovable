package com.codingshuttle.distributed_lovable.common_lib.security;

import java.util.function.Supplier;

/**
 * Carries the current request's JWT across execution boundaries (Reactor / Spring AI
 * advisor and tool execution) where Spring Security's ThreadLocal-based SecurityContext
 * is not guaranteed to still be populated.
 *
 * The token must be set immediately before, and cleared immediately after, the specific
 * blocking Feign call that needs it - see FileTreeContextAdvisor and CodeGenerationTools.
 */
public final class FeignAuthTokenContext {

    private static final ThreadLocal<String> TOKEN = new ThreadLocal<>();

    private FeignAuthTokenContext() {
    }

    public static <T> T withToken(String token, Supplier<T> action) {
        String previous = TOKEN.get();
        TOKEN.set(token);
        try {
            return action.get();
        } finally {
            if (previous != null) {
                TOKEN.set(previous);
            } else {
                TOKEN.remove();
            }
        }
    }

    public static String get() {
        return TOKEN.get();
    }
}
