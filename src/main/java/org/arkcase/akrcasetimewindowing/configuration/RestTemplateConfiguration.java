package org.arkcase.akrcasetimewindowing.configuration;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Feb, 2020
 */
@Configuration
public class RestTemplateConfiguration
{
    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder)
    {
        return builder.build();
    }
}
