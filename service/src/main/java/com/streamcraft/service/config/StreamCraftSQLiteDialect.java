package com.streamcraft.service.config;

import java.sql.Types;
import org.hibernate.community.dialect.SQLiteDialect;

/**
 * Keeps schema validation compatible with SQLite's INTEGER rowid primary key.
 * SQLite exposes that column as INTEGER even when the Java model uses Long.
 */
public class StreamCraftSQLiteDialect extends SQLiteDialect {

    @Override
    public boolean equivalentTypes(int firstTypeCode, int secondTypeCode) {
        return super.equivalentTypes(firstTypeCode, secondTypeCode)
                || isIntegerFamily(firstTypeCode) && isIntegerFamily(secondTypeCode);
    }

    private boolean isIntegerFamily(int typeCode) {
        return typeCode == Types.INTEGER || typeCode == Types.BIGINT;
    }
}
