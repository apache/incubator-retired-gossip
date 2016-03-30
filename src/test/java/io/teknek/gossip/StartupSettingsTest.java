package io.teknek.gossip;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.StartupSettings;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.junit.Test;

import io.teknek.tunit.TUnit;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests support of using {@code StartupSettings} and thereby reading
 * setup config from file.
 */
public class StartupSettingsTest {
  private static final Logger log = Logger.getLogger( StartupSettingsTest.class );

  @Test
  public void testUsingSettingsFile() throws IOException, InterruptedException, JSONException {
    File settingsFile = File.createTempFile("gossipTest",".json");
    log.debug( "Using settings file: " + settingsFile.getAbsolutePath() );
    settingsFile.deleteOnExit();
    writeSettingsFile(settingsFile);
    final GossipService firstService = new GossipService(
      "127.0.0.1", 50000, UUID.randomUUID().toString(),
      new ArrayList<GossipMember>(), new GossipSettings(), null);
    
    firstService.start();
    
    TUnit.assertThat(new Callable<Integer> (){
      public Integer call() throws Exception {
          return firstService.get_gossipManager().getMemberList().size();
      }}).afterWaitingAtMost(30, TimeUnit.SECONDS).isEqualTo(0);
    final GossipService serviceUnderTest = new GossipService(
            StartupSettings.fromJSONFile( settingsFile )
          );
    serviceUnderTest.start();
    TUnit.assertThat(new Callable<Integer> (){
      public Integer call() throws Exception {
          return serviceUnderTest.get_gossipManager().getMemberList().size();
      }}).afterWaitingAtMost(10, TimeUnit.SECONDS).isEqualTo(1);
    firstService.shutdown();
    serviceUnderTest.shutdown();
  }

  private void writeSettingsFile( File target ) throws IOException {
    String settings =
            "[{\n" + // It is odd that this is meant to be in an array, but oh well.
            "  \"id\":\"" + UUID.randomUUID() + "\",\n" +
            "  \"port\":50001,\n" +
            "  \"gossip_interval\":1000,\n" +
            "  \"cleanup_interval\":10000,\n" +
            "  \"members\":[\n" +
            "    {\"host\":\"127.0.0.1\", \"port\":50000}\n" +
            "  ]\n" +
            "}]";

    log.info( "Using settings file with contents of:\n---\n" + settings + "\n---" );
    FileOutputStream output = new FileOutputStream(target);
    output.write( settings.getBytes() );
    output.close();
  }
}
