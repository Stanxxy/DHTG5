package com.company;

import com.company.Cassandra.CaDHT;

import java.util.Scanner;

public class Main {

    // We need to maintain a DHT Object to simulate service
    BasicDHT foregroundDHT;
    BasicDHT backgroundDHT;

    public static void main(String[] args) {
	// write your code here
        Main obj = new Main();
        obj.run();
    }

    // TODO : A manu method
    private void run(){
        Scanner sc = new Scanner(System.in);
        printMenu();
        String nextOption = sc.next();
        while(!nextOption.equals("q")){
            switch(nextOption){
                case "s": selectDHT(sc);
                break;
                case ""
            }
            printMenu();
            nextOption = sc.next();
        }
    }


    private void printMenu(){
        // in this function, we simply print the menu
        System.out.println("Please select an option from the following command to go on");
        System.out.println("s -- select a DHT Type");
        System.out.println("i -- insert a dataObject");
        System.out.println("r -- retrieve a dataObject");
        System.out.println("u -- update a dataObject");
        System.out.println("d -- delete a dataObject");
        System.out.println("ln -- list existing node list");
        System.out.println("ld -- list the metadata of all stored files");
        System.out.println("an -- add node");
        System.out.println("rn -- remove nodes");
        System.out.println("lb -- ask the system to do load balance");
        System.out.println("q -- quit the program");
    }

    private void selectDHT(Scanner sc){
        System.out.println("Please input the type of DHT you want. Ca for Cassandra and Ce for Ceph:");
        String type = sc.next();
        if(type.equals("Ca")){
            if(this.backgroundDHT == null){
                this.foregroundDHT = new CaDHT();
            } else if(!this.foregroundDHT.getName().equals("CaDHT")){
                BasicDHT tmp = this.foregroundDHT;
                this.foregroundDHT = this.backgroundDHT;
                this.backgroundDHT = tmp;
            }
        } else if(type.equals("Ce")) {
            // ## do your own code here ##
        } else {
            System.out.println("No such DHT type exists");
        }
    }

    private void DBOps(Scanner sc, String op){
        String key = sc.next();
        if(op.equals("r")){
            System.out.println(this.foregroundDHT.select(key));
        } else if(op.equals("d")){
            System.out.println(this.foregroundDHT.delete(key));
        } else {
            String value = sc.next();
            if (op.equals("i")) {
                System.out.println(this.foregroundDHT.insert(key, value));
            } else if (op.equals("u")) {
                System.out.println(this.foregroundDHT.update(key, value));
            }
        }
    }

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
}