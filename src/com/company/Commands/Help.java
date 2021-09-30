package com.company.Commands;

import com.company.Main;

public class Help extends Command {
    private Command[] commands;

    public Help(Main main) {
        super(main, "h", "get help for a command", 1);
    }

    @Override
    protected void runOnLine(String[] args) {
        String commandToHelp = args[1];

        for(Command command : commands) {
            if(command.checkCalled(commandToHelp)) {
                System.out.println("Usage: " + command.getUsage());
            }
        }
    }

    public void setCommands(Command[] commands) {
        this.commands = commands;
    }
}
