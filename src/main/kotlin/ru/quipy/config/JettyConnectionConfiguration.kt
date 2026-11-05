package ru.quipy.config

import org.eclipse.jetty.server.ServerConnector
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory
import org.springframework.boot.web.server.WebServerFactoryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class JettyConnectionConfiguration {

    @Bean
    fun jettyConnectionBacklogCustomizer(): WebServerFactoryCustomizer<JettyServletWebServerFactory> =
        WebServerFactoryCustomizer { factory ->
            factory.addServerCustomizers({ server ->
                // Case 5 opens many HTTP/1.1 connections at once. The default backlog
                // falls back to 50 in the JDK, independently of the worker queue.
                server.connectors.filterIsInstance<ServerConnector>().forEach { connector ->
                    connector.acceptQueueSize = 512
                }
            })
        }
}