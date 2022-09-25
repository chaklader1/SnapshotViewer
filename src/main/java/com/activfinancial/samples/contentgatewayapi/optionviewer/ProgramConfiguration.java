/**
 * ProgramConfiguration.java  Mar 16, 2007
 *
 * Copyright 2007 ACTIV Financial Systems, Inc. All rights reserved.
 * ACTIV PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.activfinancial.samples.contentgatewayapi.optionviewer;

import java.util.ArrayList;
import java.util.List;

import com.activfinancial.middleware.StatusCode;
import com.activfinancial.middleware.activbase.MessageValidator;
import com.activfinancial.middleware.activbase.MiddlewareException;
import com.activfinancial.middleware.fieldtypes.timecommon.Resolution;
import com.activfinancial.samples.common.busobj.CommandLineParser;
import com.activfinancial.samples.common.busobj.ConfigurationParameter;
import com.activfinancial.samples.common.busobj.FieldSpecification;

/**
 * @author Ilya Goberman
 */
public class ProgramConfiguration {

	enum TimestampFormat {
		TIME_FORMAT_SECONDS,
		TIME_FORMAT_MILLISECONDS,
		TIME_FORMAT_MICROSECONDS,
		TIME_FORMAT_NANOSECONDS,
		TIME_FORMAT_RECIEVED,
		TIME_FORMAT_SMART;

		enum TimestampInitials {
			S,
			M,
			U,
			N,
			R,
			T
		}

		public static TimestampFormat fromString(String property) throws MiddlewareException {
			if (property == null)
				return TIME_FORMAT_SMART;

			TimestampInitials timestampInitial = TimestampInitials.valueOf(property.toUpperCase());

			switch (timestampInitial) {
				case S:
					return TIME_FORMAT_SECONDS;

				case M:
					return TIME_FORMAT_MILLISECONDS;

				case U:
					return TIME_FORMAT_MICROSECONDS;

				case N:
					return TIME_FORMAT_NANOSECONDS;

				case R:
					return TIME_FORMAT_RECIEVED;

				case T:
					return TIME_FORMAT_SMART;

				default:
					throw new MiddlewareException(StatusCode.STATUS_CODE_INVALID_PARAMETER);
			}
		}
	}

	ProgramConfiguration() {
		bestResolution = Resolution.RESOLUTION_SECOND;
	}

    // The service location ini file.
	private String serviceLocationIniFile;

    // The service id.
	private String serviceId;

    // id of a server as defined in the ServiceLocation.xml
    private String serviceInstanceId;

    // The user id.
	private String userId;

    // The password.
	private String password;

    // The table number.
	private char tableNumber;

    // The symbol.
	private List<String> symbolList;

    // The field specification list.
	private List<FieldSpecification> fieldSpecificationList;

    // The field specification list to ignore.
    private List<FieldSpecification> ignoreFieldSpecificationList;


    // Indicates whether the updates should be delayed.
	private boolean isDelayed;

    // Indicates whether the extended trended rational information should be displayed.
	private boolean isExtendedTRationalDisplay;

    // filed separator
	private String separator;

    // file to generate data to
	private String outputFile;

    // Indicates whether the alias is ignored.
	private boolean isIgnoreAlias;

    // The host to connect to.
    private String host;

	private Resolution bestResolution;

	private TimestampFormat fieldTimestampFormat;

    boolean process(String[] args) throws Exception {
        List<ConfigurationParameter> parameters = new ArrayList<ConfigurationParameter>();

        // mandatory
        parameters.add(new ConfigurationParameter("U", "drwt1000-dwashok","ACTIV feed user id", "setUserId"));
        parameters.add(new ConfigurationParameter("P", "dwashok","ACTIV feed password", "setPassword"));

        // optional
        parameters.add(new ConfigurationParameter("S", "META/*.O", "Semi-colon delimited list of symbol patterns to subscribe", "setSymbol"));
        parameters.add(new ConfigurationParameter("L", "/Users/chaklader/IdeaProjects/SnapshotViewer/ServiceLocation.xml", "Service location ini file to use", "setServiceLocationIniFile"));
        parameters.add(new ConfigurationParameter("N", null, "Network to connect to", "setServiceInstanceId"));
        parameters.add(new ConfigurationParameter("I", "Service.ContentGateway", "Service id to connect to", "setServiceId"));
        parameters.add(new ConfigurationParameter("H", null, "Host to connect to with optional port i.e. hostname[:9002] or a.b.c.d[:9002]", "setHost"));

        parameters.add(new ConfigurationParameter("D", "false", "Subscribe to delayed feed rather than realtime", "setDelayed"));
        parameters.add(new ConfigurationParameter("E", "false", "Whether to display trending information with TRational fields", "setExtendedTRationalDisplay"));
        parameters.add(new ConfigurationParameter("A", "false", "Don't resolve aliases to their target symbols", "setIgnoreAlias"));
        parameters.add(new ConfigurationParameter("C", ",", "Character to use as field separator in output file", "setSeparator"));
        parameters.add(new ConfigurationParameter("O", "/Users/chaklader/IdeaProjects/SnapshotViewer/data_meta.csv", "File to log updates to", "setOutputFile"));
		parameters.add(new ConfigurationParameter("field-timestamp-format", "T", "The field timestamp format, S = seconds, M = milliseconds, U = microseconds, N = nanoseconds, R = received, T = smart", "setFieldTimestampFormat"));


        // field specification list
        parameters.add(new ConfigurationParameter("F", "*", "Semi-colon delimited list of field ids", "setFieldIdList"));
        // ignore field specification list
        parameters.add(new ConfigurationParameter("FI", "", "Semi-colon delimited list of fields to ignore i.e. 456;523", "setIgnoreFieldIdList"));

        parameters.add(new ConfigurationParameter("T", "1", "Table number to subscribe to", "setTableNumber"));

        if (args.length == 0) {
            for (ConfigurationParameter parameter : parameters) {
                CommandLineParser.parse(this, parameter);
            }
            return true;
        }
        else if (args.length == 1 && (args[0].equals("-?") || args[0].equals("/?"))) {
            CommandLineParser.displayHelp(parameters);
            return false;
        }
        else {
            throw new Exception("Unknown command line option.");
        }
	}

    public void setFieldIdList(String fieldSpecificationString) {
        this.fieldSpecificationList = new ArrayList<FieldSpecification>();
        if (fieldSpecificationString.length() != 0) {
            getFieldSpecificationList(fieldSpecificationString, fieldSpecificationList);
        }
    }

    public void setIgnoreFieldIdList(String ignoreFieldSpecificationString) {
        this.ignoreFieldSpecificationList = new ArrayList<FieldSpecification>();
        if (ignoreFieldSpecificationString.length() != 0) {
            getFieldSpecificationList(ignoreFieldSpecificationString, ignoreFieldSpecificationList);
        }
    }

    public void setTableNumber(char tableNumber) {
        this.tableNumber = tableNumber;
    }

	private boolean getFieldSpecificationList(String fieldSpecificationListString, List<FieldSpecification> fieldSpecificationList) {
		fieldSpecificationList.clear();
		if (!fieldSpecificationListString.equals("*")) {
			MessageValidator messageValidator = new MessageValidator();

			try {
				final byte[] fieldSpecificationListBytes = fieldSpecificationListString.getBytes();

				for (int offset = 0, lastOffset = 0; offset < fieldSpecificationListString.length(); ++offset) {
					FieldSpecification fieldSpecification = new FieldSpecification();

					for (; (offset < fieldSpecificationListString.length()) && (':' != fieldSpecificationListString.charAt(offset)) && (';' != fieldSpecificationListString.charAt(offset)); ++offset);

					if (offset <= lastOffset)
						return false;

					if ((offset < fieldSpecificationListString.length()) && (':' == fieldSpecificationListString.charAt(offset))) {
						messageValidator.initialize(fieldSpecificationListBytes, offset - lastOffset, 0, lastOffset);
						fieldSpecification.setRelationshipId(messageValidator.validateUnsignedAsciiIntegralShort(offset - lastOffset));

						lastOffset = offset = offset + 1;

						for (; (offset < fieldSpecificationListString.length()) && (';' != fieldSpecificationListString.charAt(offset)); ++offset);

						if (offset <= lastOffset)
							return false;
					}

					messageValidator.initialize(fieldSpecificationListBytes, offset - lastOffset, 0, lastOffset);
					fieldSpecification.setFieldId(messageValidator.validateUnsignedAsciiIntegralShort(offset - lastOffset));

					lastOffset = offset + 1;

					fieldSpecificationList.add(fieldSpecification);
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}

		return true;
	}

	public List<FieldSpecification> getFieldSpecificationList() {
		return fieldSpecificationList;
	}

    public List<FieldSpecification> getIgnoreFieldSpecificationList() {
        return ignoreFieldSpecificationList;
    }

	public boolean isDelayed() {
		return isDelayed;
	}

	public boolean isExtendedTRationalDisplay() {
		return isExtendedTRationalDisplay;
	}

	public boolean isIgnoreAlias() {
		return isIgnoreAlias;
	}

	public String getPassword() {
		return password;
	}

	public String getServiceId() {
		return serviceId;
	}

	public String getServiceLocationIniFile() {
		return serviceLocationIniFile;
	}

	public List<String> getSymbolList() {
		return symbolList;
	}

	public char getTableNumber() {
		return tableNumber;
	}

	public String getUserId() {
		return userId;
	}

	public String getOutputFile() {
		return outputFile;
	}

	public String getSeparator() {
		return separator;
	}

    public String getServiceInstanceId() {
        return serviceInstanceId;
    }

    public void setDelayed(boolean isDelayed) {
        this.isDelayed = isDelayed;
    }

    public void setExtendedTRationalDisplay(boolean isExtendedTRationalDisplay) {
        this.isExtendedTRationalDisplay = isExtendedTRationalDisplay;
    }

    public void setIgnoreAlias(boolean isIgnoreAlias) {
        this.isIgnoreAlias = isIgnoreAlias;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public void setServiceInstanceId(String serviceInstanceId) {
        this.serviceInstanceId = serviceInstanceId;
    }

    public void setServiceLocationIniFile(String serviceLocationIniFile) {
        this.serviceLocationIniFile = serviceLocationIniFile;
    }

    public boolean setSymbol(String symbolListString) {
    	symbolList = new ArrayList<String>();
        if (!symbolListString.equals("*"))
        {
        	for (int offset = 0, lastOffset = 0; offset <= symbolListString.length(); ++offset)
			{
				StringBuilder symbol = new StringBuilder();

				boolean isEscaped = false;


				for (; (offset < symbolListString.length()) && ((';' != symbolListString.charAt(offset)) || ((0 != offset) && ('\\' == symbolListString.charAt(offset-1)))); ++offset)
				{
					if (isEscaped)
					{
						symbol.append(symbolListString.charAt(offset));
						isEscaped = false;
					}
					else if ('\\' == symbolListString.charAt(offset))
					{
						isEscaped = true;
					}
					else
					{
						symbol.append(symbolListString.charAt(offset));
					}
				}

				if (offset <= lastOffset)
					return false;

				lastOffset = offset + 1;

				symbolList.add(symbol.toString());
			}
        }

        return true;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        if (host != null && host.length() != 0) {
            this.host = host + ((host.indexOf(":") != -1) ? "" : ":9002");
        }
    }

	public TimestampFormat getFieldTimestampFormat() {
		return fieldTimestampFormat;
	}

	public void setFieldTimestampFormat(String format) throws MiddlewareException {
		this.fieldTimestampFormat = TimestampFormat.fromString(format);
	}

	public Resolution getBestResoltion() {
		return bestResolution;
	}

	public void setBestResoltion(Resolution resolution) {
		this.bestResolution = resolution;
	}
}
