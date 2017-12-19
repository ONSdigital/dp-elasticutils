package com.github.onsdigital.elasticutils.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 *
 * Utility class for converting entities to byte arrays
 */
public class JsonUtils {

    private static Logger LOGGER = LoggerFactory.getLogger(JsonUtils.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static <T> Optional<byte[]> convertJsonToBytes(T entity) {
        try {
            return Optional.empty().of(MAPPER.writeValueAsBytes(entity));
        } catch(Exception e) {
            if(LOGGER.isErrorEnabled()) {
                LOGGER.error(String.format("Failed to convert entity %s to byte array", entity), e);
            }
        }
        return Optional.empty();
    }

    public static <T> Optional<byte[]> convertJsonToBytes(T entity, JsonInclude.Include include) {
        try {
            if (include.equals(JsonInclude.Include.USE_DEFAULTS)) {
                return Optional.empty().of(MAPPER.writeValueAsBytes(entity));
            } else {
                ObjectMapper customMapper = new ObjectMapper();
                customMapper.setSerializationInclusion(include);
                return Optional.empty().of(customMapper.writeValueAsBytes(entity));
            }

        } catch(Exception e) {
            if(LOGGER.isErrorEnabled()) {
                LOGGER.error(String.format("Failed to convert entity %s to byte array", entity), e);
            }
        }
        return Optional.empty();
    }

}
