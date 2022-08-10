package com.hassialis.philip.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.LocalTime;

@Controller("/time")
public class RateLimitedTimeEndpoint {

	private static final int QUOTA_PER_MINUTE = 10;
	private static final Logger LOG = LoggerFactory.getLogger(RateLimitedTimeEndpoint.class);
	private StatefulRedisConnection<String, String> redis;

	public RateLimitedTimeEndpoint(StatefulRedisConnection<String, String> redis) {
		this.redis = redis;
	}

	@ExecuteOn(TaskExecutors.IO)
	@Get(uri = "/", produces = MediaType.TEXT_PLAIN)
	public String time() {
		return getTime("EXAMPLE::TIME",LocalTime.now());
	}

	@ExecuteOn(TaskExecutors.IO)
	@Get(uri = "/utc", produces = MediaType.TEXT_PLAIN)
	public String utc() {
		return getTime("EXAMPLE::UTC",  LocalTime.now(Clock.systemUTC()));
	}

	private String getTime(final String key, final LocalTime now) {
		final String value = redis.sync().get(key);
		int currentQuota = null == value ? 0 : Integer.parseInt(value);
		if (currentQuota >= QUOTA_PER_MINUTE) {
			final String err = String.format("Rate limit reached %s %s/%s",key, currentQuota, QUOTA_PER_MINUTE);
			LOG.info(err);
			return err;
		}
		LOG.info("Current quota {} {}/{}", key, currentQuota, QUOTA_PER_MINUTE);
		increaseCurrentQuota(key);
		return now.toString();
	}


	private void increaseCurrentQuota(String key) {
		final RedisCommands<String, String> commands = redis.sync();
		// multi
		commands.multi();
		// increase
		commands.incrby(key, 1);
		// expire
		var remainingSeconds = 60 - LocalTime.now().getSecond();
		commands.expire(key, remainingSeconds);
		// execute
		commands.exec();
	}
}
