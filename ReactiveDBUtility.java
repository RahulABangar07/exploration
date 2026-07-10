package com.example.util;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class ReactiveDBUtility {

    @FunctionalInterface
    public interface ProcedureExecutor<T> {
        T execute(CallableStatement cstmt) throws SQLException;
    }

    /**
     * Executes a stored procedure asynchronously on an elastic thread pool, 
     * preserving HikariCP connection retrieval and token processing.
     */
    public static <T> Flux<T> executeStoredProcedureFlux(
            DataSource dataSource, 
            String sql, 
            ProcedureExecutor<Iterable<T>> executor, 
            Object... params) {
        
        // Mono.fromCallable safely wraps the blocking creation sequence
        return Mono.fromCallable(() -> {
            
            // 1. HikariCP intercepts here, validating/refreshing RBAC tokens safely
            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall(sql)) {
                
                // Generic binding loop
                if (params != null) {
                    for (int i = 0; i < params.length; i++) {
                        Object param = params[i];
                        if (param == null) {
                            cstmt.setNull(i + 1, java.sql.Types.NULL);
                        } else {
                            cstmt.setObject(i + 1, param);
                        }
                    }
                }
                
                // Hand control over to the Service implementation
                return executor.execute(cstmt);
            }
        })
        // 2. Offloads blocking IO from the reactive Netty Event Loop
        .subscribeOn(Schedulers.boundedElastic()) 
        // 3. Flattens out the Iterable result stream into a native Flux
        .flatMapMany(Flux::fromIterable); 
    }

    /**
     * Specialized execution variant for Mutation operations (Updates/Inserts) that return a singular row count.
     */
    public static Mono<Integer> executeStoredProcedureMono(
            DataSource dataSource, 
            String sql, 
            ProcedureExecutor<Integer> executor, 
            Object... params) {
        
        return Mono.fromCallable(() -> {
            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall(sql)) {
                
                if (params != null) {
                    for (int i = 0; i < params.length; i++) {
                        if (params[i] == null) {
                            cstmt.setNull(i + 1, java.sql.Types.NULL);
                        } else {
                            cstmt.setObject(i + 1, params[i]);
                        }
                    }
                }
                return executor.execute(cstmt);
            }
        })
        .subscribeOn(Schedulers.boundedElastic());
    }
}
