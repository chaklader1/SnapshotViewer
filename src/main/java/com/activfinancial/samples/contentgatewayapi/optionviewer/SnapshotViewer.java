/**
 * SnapshotViewer.java  Mar 16, 2007
 * <p>
 * Copyright 2007 ACTIV Financial Systems, Inc. All rights reserved.
 * ACTIV PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.activfinancial.samples.contentgatewayapi.optionviewer;

import com.activfinancial.middleware.activbase.MiddlewareException;
import com.activfinancial.middleware.application.Application;
import com.activfinancial.middleware.application.Settings;

/**
 * @author Ilya Goberman
 */
public class SnapshotViewer {
    /**
     * main entry point into application
     * @param args
     */
    public static void main(String[] args) {
        new SnapshotViewer().run(args);
    }

    // Activ application instance
    private Application application;
    @SuppressWarnings("unused")
    private SnapshotViewerContentGatewayClient contentGatewayClient;

    // application entry point
    private void run(String[] args) {
        // parse user supplied arguments.
        ProgramConfiguration programConfiguration = new ProgramConfiguration();
        try {
            if (!programConfiguration.process(args)) {
                return;
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // construct application settings
        Settings settings = new Settings();
        settings.serviceLocationIniFile = programConfiguration.getServiceLocationIniFile();

        try {
            // construct new application.
            application = new Application(settings);

            contentGatewayClient = new SnapshotViewerContentGatewayClient(application, programConfiguration);

            // run application
            application.run();

        } catch (MiddlewareException e) {
            System.out.println(e.toString());
            if (application != null)
                application.postDiesToThreads();
        }
    }
}
