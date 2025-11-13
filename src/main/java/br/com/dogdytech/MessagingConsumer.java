package br.com.dogdytech;

import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.io.IOException;

@ApplicationScoped
public class MessagingConsumer {
	@Incoming("consumer")
	public void consumeEvent(JsonObject json) throws IOException {
		MessagingDTO dto = json.mapTo(MessagingDTO.class);

		System.out.println("A message was received in the script scheduling consumer: " + dto.getTitle());
	}
}
