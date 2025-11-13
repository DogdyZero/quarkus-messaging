package br.com.dogdytech;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

@Path("/messaging")
public class MessagingController {
	@Inject
	MessagingProducer messagingProducer;

	@POST
	public Response sendMessage(MessagingDTO dto) {
		messagingProducer.sendMessage(dto);
		return Response.ok()
				.build();
	}

	@GET
	public Response healthCheck() {
		return Response.ok()
				.build();
	}
}
