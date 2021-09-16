package com.test.flink.broadcast;

import lombok.*;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class TrackLog {

	private String userId ;
	private Integer productId ;
	private String trackTime ;
	private String eventType ;

	@Override
	public String toString() {
		return userId + ", " + productId + ", " + trackTime + ", " + eventType;
	}
}
