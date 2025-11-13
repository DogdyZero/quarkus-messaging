package br.com.dogdytech;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class MessagingProducer {

	@Inject
	@Channel("producer")
	Emitter<MessagingDTO> agendamentoScriptEmitter;

	public void sendMessage(MessagingDTO dto) {
		agendamentoScriptEmitter.send(dto);
	}
}
