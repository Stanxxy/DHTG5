package com.company;

import com.company.ceph.CeCluster;
import com.company.ceph.CeNode;
import com.company.ceph.CephHashTools;
import com.company.commands.*;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
	// write your code here
        Main obj = new Main();
        obj.start();
    }

    // We need to maintain a DHT Object to simulate service
    public BasicDHT foregroundDHT;
    public BasicDHT backgroundDHT;
    public NodeManager foregroundManager;
    public NodeManager backgroundManager;

    private Command[] commands;
    private Scanner terminal;
    private boolean running;

    public Main() {
        Help helpCommand = new Help(this);
        commands = new Command[]{
                helpCommand,
                new SelectDHT(this),
                new SetClusterMeta(this),
                new ListAllNodes(this),
                new ListNodeData(this),
                new Quit(this),
                new Insert(this),
                new Retrieve(this),
        };
        helpCommand.setCommands(commands);
        terminal = new Scanner(System.in);
        running = false;
    }

    public void start() {
        running = true;
        run();
    }

    // TODO : A manu method

    private void run() {
        while(running) {
            Command.printMenu(commands);
            try {
                Command.runLine(commands, terminal.nextLine());
            } catch(Exception e) {
                e.printStackTrace();
                // haha the command ran but messed up... try again!
            }
        }
    }

    public void stop() {
        running = false;
    }

    public Scanner getScanner() {
        return terminal;
    }
}