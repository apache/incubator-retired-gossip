package com.google.code.gossip;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.json.JSONException;

public class GossipRunner {
	private StartupSettings _settings;
	
	public static void main(String[] args) {
		File configFile;
		
		if (args.length == 1) {
			configFile = new File("./"+args[0]);
		} else {
			configFile = new File("gossip.conf");
		}
		
		new GossipRunner(configFile);
	}
	
	public GossipRunner(File configFile) {
		
		if (configFile != null && configFile.exists()) {
			try {
				System.out.println("Parsing the configuration file...");
				_settings = StartupSettings.fromJSONFile(configFile);
				GossipService gossipService = new GossipService(_settings);
				System.out.println("Gossip service successfully inialized, let's start it...");
				gossipService.start();
			} catch (FileNotFoundException e) {
				System.err.println("The given file is not found!");
			} catch (JSONException e) {
				System.err.println("The given file is not in the correct JSON format!");
			} catch (IOException e) {
				System.err.println("Could not read the configuration file: " + e.getMessage());
			} catch (InterruptedException e) {
				System.err.println("Error while starting the gossip service: " + e.getMessage());
			}
		} else {
			System.out.println("The gossip.conf file is not found.\n\nEither specify the path to the startup settings file or place the gossip.json file in the same folder as the JAR file.");
		}
	}
}
