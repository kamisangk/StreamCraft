package com.streamcraft.core.runtime.transform.custom;

import com.streamcraft.core.model.DataEntity;
import java.io.Serializable;

public interface CustomTransform extends Serializable {

    DataEntity process(DataEntity input, CustomTransformContext context) throws Exception;
}
