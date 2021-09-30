package com.company;

import com.company.Commands.*;

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
                new Insert(this),
                new Retrieve(this),
                new Update(this),
                new Delete(this),
                new AddNode(this),
                new RemoveNode(this),
                new UnplugNode(this),
                new BalanceLoad(this),
                new SetClusterMeta(this),
                new ListAllNodes(this),
                new ListNodeData(this),
                new ListNodeMeta(this),
                new Quit(this)
        };
        helpCommand.setCommands(commands);
        terminal = new Scanner(System.in);
        running = false;
    }

    public void start() {
        running = true;
        run();
    }

    private void run() {
        while(running) {
            Command.printMenu(commands);
            Command.runLine(commands, terminal.nextLine());
        }
    }

    public void stop() {
        running = false;
    }

    public Scanner getScanner() {
        return terminal;
    }
}