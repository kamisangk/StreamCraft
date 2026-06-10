package com.streamcraft.core.runtime.transform.custom;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.fields.FieldPathSupport;
import java.io.Serializable;

public final class CustomTransformContext implements Serializable {

    private static final long serialVersionUID = 1L;

    public Object get(DataEntity input, String fieldPath) {
        FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(input.fields(), fieldPath);
        return lookup.found() ? lookup.value() : null;
    }

    public boolean has(DataEntity input, String fieldPath) {
        return FieldPathSupport.lookup(input.fields(), fieldPath).found();
    }

    public DataEntity set(DataEntity input, String fieldPath, Object value) {
        return input.withField(fieldPath, value);
    }

    public DataEntity remove(DataEntity input, String fieldPath) {
        return input.withoutField(fieldPath);
    }
}
