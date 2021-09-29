package com.company.Commands;

import com.company.Main;

public abstract class Command {
    protected Main main;
    protected String commandText;
    protected String description;
    protected int parameterMinimum;

    protected Command(Main main, String commandText, String description, int parameterMinimum) {
        this.main = main;
        this.commandText = commandText;
        this.description = description;
        this.parameterMinimum = parameterMinimum;
    }

    public static void printMenu(Command[] commands) {
        System.out.println("Please select an option from the following commands");
        for(Command command : commands) {
            System.out.println(command.toString());
        }
        System.out.print("> ");
    }

    public static void runLine(Command[] commands, String line) {
        String[] split = line.split(" ");

        if(split.length > 0) {
            for(Command command : commands) {
                if(command.checkCalled(split[0])) {
                    if(command.checkValid(split)) {
                        command.runOnLine(split);
                        return;
                    }
                    System.out.println("Not enough arguments for this command");
                    return;
                }
            }
            System.out.println("No such command");
            return;
        }
        System.out.println("No line entered");
    }

    public boolean checkCalled(String commandText) {
        return this.commandText.equals(commandText);
    }

    public boolean checkValid(String[] args) {
        return args.length - 1 >= parameterMinimum;
    }

    protected abstract void runOnLine(String[] args);

    public String toString() {
        return commandText + "\t" + description;
    }

    public String getUsage() {
        return commandText + " <" + parameterMinimum + " parameters>";
    }
}
