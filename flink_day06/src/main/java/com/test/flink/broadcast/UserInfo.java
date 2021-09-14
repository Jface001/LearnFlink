package com.test.flink.broadcast;

import lombok.*;

@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class UserInfo {

	private String userId ;
	private String userName ;
	private Integer userAge ;

	@Override
	public String toString() {
		return userId + ", " + userName + ", " + userAge ;
	}
}
