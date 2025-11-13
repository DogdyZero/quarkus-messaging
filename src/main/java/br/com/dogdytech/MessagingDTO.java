package br.com.dogdytech;

public class MessagingDTO {
	private String title;
	private String description;
	private String event;

	public String getDescription() {
		return description;
	}

	public String getTitle() {
		return title;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}
}
