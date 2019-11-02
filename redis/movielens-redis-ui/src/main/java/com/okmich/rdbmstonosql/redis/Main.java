/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.rdbmstonosql.redis;

import com.okmich.rdbmstonosql.redis.ui.AppFrame;
import javax.swing.JOptionPane;

/**
 *
 * @author Michael Enudi
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {

        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(AppFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }

        //
        String s = (String) JOptionPane.showInputDialog(
                null,
                "Enter the host and port number of a redis server. \n"
                + "Format is host:port. \n"
                + "Example is localhost:6379.",
                "Movielens on Redis",
                JOptionPane.QUESTION_MESSAGE,
                null,
                null,
                "localhost:6379");

        if (s == null || !s.contains(":")) {
            System.exit(0);
        }

        if (!s.contains(":")) {
            JOptionPane.showMessageDialog(null, "Invalid address port combination.\n Try localhost:6379", "", JOptionPane.ERROR_MESSAGE);
            System.exit(0);
        }

        String[] parts = s.split(":");
        int port = Integer.parseInt(parts[1]);

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(() -> {
            new AppFrame(parts[0], port).setVisible(true);
        });
    }

}
