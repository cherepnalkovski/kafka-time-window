package org.arkcase.akrcasetimewindowing.service;

import com.google.gson.JsonObject;

import java.time.Instant;
import java.util.UUID;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Apr, 2020
 */
public class MessageGeneratorNext
{
    public static JsonObject create(String tenant)
    {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("id", UUID.randomUUID().toString());
            jsonObject.addProperty("eventDate", String.valueOf(Instant.now().toEpochMilli()));
            jsonObject.addProperty("userId", "vladimir.cherepnalkovski@armedia.com");
            jsonObject.addProperty("tenantId", tenant);
            jsonObject.addProperty("eventType", "Auth-Event");
            jsonObject.addProperty("status", "Success");
            return jsonObject;
    }
}
