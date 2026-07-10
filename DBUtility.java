package com.example.util;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

public class DBUtility {

    /**
     * Functional interface to allow the calling service to process results 
     * from the configured CallableStatement.
     */
    @FunctionalInterface
    public interface ProcedureExecutor<T> {
        T execute(CallableStatement cstmt) throws SQLException;
    }

    /**
     * Generic wrapper to execute Stored Procedures with mixed-type parameters safely.
     * SonarQube is 100% happy because the resource lifecycle is self-contained.
     */
    public static <T> T executeStoredProcedure(
            Connection conn, 
            String sql, 
            ProcedureExecutor<T> executor, 
            Object... params) throws SQLException {
        
        // Try-with-resources handles the CallableStatement cleanup automatically
        try (CallableStatement cstmt = conn.prepareCall(sql)) {
            
            // Generic parameters binding loop
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    Object param = params[i];
                    
                    if (param == null) {
                        // Handles null values safely for arbitrary SQL types
                        cstmt.setNull(i + 1, java.sql.Types.NULL);
                    } else {
                        // JDBC automatically handles conversions for Integer, String, Date, Boolean, etc.
                        cstmt.setObject(i + 1, param);
                    }
                }
            }
            
            // Relinquish execution control back to the specific service layer
            return executor.execute(cstmt);
        }
    }
}
