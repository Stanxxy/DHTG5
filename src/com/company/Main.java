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
                new Quit(this),
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
            Command.runLine(commands, terminal.nextLine());
        }
    }

    public void stop() {
        running = false;
    }

    public Scanner getScanner() {
        return terminal;
    }

    /*
    private void printMenu() {
        // in this function, we simply print the menu
        System.out.println("ln -- list existing node list");
        System.out.println("ld -- list the metadata of all stored files");
        System.out.println("an -- add node");
        System.out.println("rn -- remove nodes");
        System.out.println("lb -- ask the system to do load balance");
    }

     */

    /*
    private void listNode(){

    }

    private void listData(){

    }

    private void addNode(){

    }

    private void removeNode(){

    }

    private void loadBalance(){

    }
    */
}