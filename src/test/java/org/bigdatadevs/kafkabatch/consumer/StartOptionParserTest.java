package org.bigdatadevs.kafkabatch.consumer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @author marinapopova
 * Apr 2, 2018
 */
public class StartOptionParserTest {

	@Test
	public void testRestartOption() {
		StartOption startOption = StartOptionParser.getStartOption("RESTART");
		assertEquals(StartOption.RESTART, startOption);
	}

	@Test
	public void testEarliestOption() {
		StartOption startOption = StartOptionParser.getStartOption("EARLIEST");
		assertEquals(StartOption.EARLIEST, startOption);
	}

	@Test
	public void testLatestOption() {
		StartOption startOption = StartOptionParser.getStartOption("LATEST");
		assertEquals(StartOption.LATEST, startOption);
	}

	@Test
	public void testCustomOption() {
		StartOption startOption = StartOptionParser.getStartOption("CUSTOM");
		assertEquals(StartOption.CUSTOM, startOption);
	}

	@Test
	public void testWrongOption() {
		assertThrows(IllegalArgumentException.class, () ->
			StartOptionParser.getStartOption("sferggbgg")
		);
	}

	@Test
	public void testEmptyOption() {
		StartOption startOption = StartOptionParser.getStartOption("");
		assertEquals(StartOption.RESTART, startOption);
	}

	@Test
	public void testCustomOptionsFromFile() {
		Map<Integer, Long> expectedMap = new HashMap<>();
		expectedMap.put(0, 10L);
		expectedMap.put(1, 20L);
		Map<Integer, Long> resultMap = StartOptionParser.getCustomStartOffsets("src/test/resources/test-start-options-custom.properties");
		assertNotNull(resultMap);
		assertEquals(expectedMap, resultMap);
	}

	@Test
	public void testCustomOptionsFromMalformedFile() {
		Map<Integer, Long> configMap = StartOptionParser.getCustomStartOffsets("src/test/resources/test-start-options-custom-malformed.properties");
		assertNotNull(configMap);
		assertEquals(configMap.size(), 0);
	}

	@Test
	public void testCustomOptionsFromEmptyFile() {
		Map<Integer, Long> configMap = StartOptionParser.getCustomStartOffsets("src/test/resources/test-start-options-custom-empty.properties");
		assertNotNull(configMap);
		assertEquals(configMap.size(), 0);
	}
}
