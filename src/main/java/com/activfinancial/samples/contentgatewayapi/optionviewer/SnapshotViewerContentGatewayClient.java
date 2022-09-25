/**
 * SnapshotViewerContentGatewayClient.java  Mar 19, 2007
 * <p>
 * Copyright 2007 ACTIV Financial Systems, Inc. All rights reserved.
 * ACTIV PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.activfinancial.samples.contentgatewayapi.optionviewer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.activfinancial.contentplatform.contentgatewayapi.ContentGatewayClient;
import com.activfinancial.contentplatform.contentgatewayapi.GetEqual;
import com.activfinancial.contentplatform.contentgatewayapi.GetFirst;
import com.activfinancial.contentplatform.contentgatewayapi.GetNext;
import com.activfinancial.contentplatform.contentgatewayapi.FieldListValidator;
import com.activfinancial.contentplatform.contentgatewayapi.common.*;
import com.activfinancial.contentplatform.contentgatewayapi.consts.FieldStatus;
import com.activfinancial.contentplatform.contentgatewayapi.consts.PermissionLevel;
import com.activfinancial.contentplatform.contentgatewayapi.consts.RelationshipIds;
import com.activfinancial.contentplatform.contentgatewayapi.reconnects.ReconnectPolicyRetry;
import com.activfinancial.middleware.StatusCode;
import com.activfinancial.middleware.activbase.MiddlewareException;
import com.activfinancial.middleware.application.Application;
import com.activfinancial.middleware.fieldtypes.*;
import com.activfinancial.middleware.fieldtypes.timecommon.Resolution;
import com.activfinancial.middleware.misc.SimplePattern;
import com.activfinancial.middleware.service.FileConfiguration;
import com.activfinancial.middleware.service.ServiceApi;
import com.activfinancial.middleware.service.ServiceInstance;
import com.activfinancial.middleware.system.HeapMessage;
import com.activfinancial.middleware.system.RequestId;
import com.activfinancial.samples.common.busobj.FieldSpecification;
//import com.activfinancial.samples.contentgatewayapi.snapshotviewer.ProgramConfiguration.TimestampFormat;

/**
 * SnapshotViewerContentGatewayClient implements connectivity to the CG
 *
 * @author Ilya Goberman
 */
public class SnapshotViewerContentGatewayClient extends ContentGatewayClient {
	private static final int CONNECT_RETRY_TIMEOUT = 5;

	// The field specification list.
	private List<FieldSpecification> fieldSpecificationList;

	// The field specification list to ignore.
	private List<FieldSpecification> ignoreFieldSpecificationList;

	// The symbol and symbol list.
	private String symbol;
	private ListIterator<String> symbolListIterator;
	private SimplePattern symbolPattern;

	// filed separator
	private String separator;

	// The field list validator.
	private FieldListValidator fieldListValidator;

	// The last symbol.
	private String lastSymbol;

	// Indicates the relationship id is none.
	private boolean isRelationshipIdNone;

	// The request block list.
	private List<RequestBlock> requestBlockList;

	private Map<String, Map<FieldSpecification, String>> symbolToFieldKeyToValueMap;

	private BufferedOutputStream file;

	private int reconnects;
	private final static int MAX_RECONNECTS = 3;

	private ProgramConfiguration config;

	// constructor
	SnapshotViewerContentGatewayClient(Application application, ProgramConfiguration config) throws MiddlewareException {
		super(application);

		this.config = config;

		lastSymbol = "";

		isRelationshipIdNone = true;

		this.symbolListIterator = config.getSymbolList().listIterator();
		this.symbolPattern = new SimplePattern();

		this.fieldSpecificationList = config.getFieldSpecificationList();

		this.ignoreFieldSpecificationList = config.getIgnoreFieldSpecificationList();

		this.fieldListValidator = new FieldListValidator(this);

		this.separator = config.getSeparator();

		this.requestBlockList = new ArrayList<RequestBlock>();

		this.symbolToFieldKeyToValueMap = new TreeMap<String, Map<FieldSpecification, String>>();

		if (config.getOutputFile() != null && config.getOutputFile().length() != 0) {
			try {
				this.file = new BufferedOutputStream(new FileOutputStream(new File(config.getOutputFile()), false));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		final StatusCode statusCode = connect(true);
		if (statusCode != StatusCode.STATUS_CODE_SUCCESS)
			throw new MiddlewareException(statusCode);

		if (postFirstRequest() != StatusCode.STATUS_CODE_SUCCESS)
			return;
	}

	// connect to the CG
	private StatusCode connect(boolean syncConnect) {


		StatusCode statusCode;

		ConnectParameters connectParameters = new ConnectParameters();
		if (config.getHost() != null) {
//			connectParameters.url = String.format("ams://%s/ContentGateway:Service?rxCompression=Rdc", config.getHost());
			connectParameters.url = "ams://199.47.167.20:9005/ContentGateway:Service?rxCompression=Rdc";
		} else {
			// First stage to connect is to find a service to connect to
			// we need to resolve a service id (eg "Service.ContentGateway") to a list of service instances with that id.
			// each instance of a service has a list of access points associated with it.
			List<ServiceInstance> serviceInstanceList = new ArrayList<ServiceInstance>();

			Map<String, Object> attributes = new HashMap<String, Object>();
			attributes.put(FileConfiguration.FILE_LOCATION, getApplication().getSettings().serviceLocationIniFile);
			statusCode = ServiceApi.findServices(ServiceApi.CONFIGURATION_TYPE_FILE, config.getServiceId(), attributes, serviceInstanceList);

			if (StatusCode.STATUS_CODE_SUCCESS != statusCode) {
				System.out.println("FindServices() failed, error - " + statusCode.toString());
				return statusCode;
			}

			// here we are just going to pick the first service that is returned, and its first access point url
			ServiceInstance serviceInstance = serviceInstanceList.get(0);

			if (config.getServiceInstanceId() != null) {
				for (ServiceInstance si : serviceInstanceList) {
					if (si.serviceAccessPointList.get(0).id.equals(config.getServiceInstanceId())) {
						serviceInstance = si;
						break;
					}
				}
			}
			connectParameters.url = serviceInstance.serviceAccessPointList.get(0).url;
		}

		connectParameters.serviceId = config.getServiceId();
		connectParameters.userId = config.getUserId();
		connectParameters.password = config.getPassword();

		// set reconnect policy to reconnect automatically to the Content Gateway.
		setReconnectPolicy(new ReconnectPolicyRetry(connectParameters, 0 /* reconnect delay after onBreak */, CONNECT_RETRY_TIMEOUT * 1000 /* reconnect delay after onConnectFailed */));

		statusCode = super.connect(connectParameters, syncConnect ? DEFAULT_TIMEOUT : 0);

		if (StatusCode.STATUS_CODE_SUCCESS != statusCode)
			System.out.println("Connect() failed, error - " + statusCode.toString());

		return statusCode;
	}

	/**
	 * invoked when asynchronous connect to the data source is successful
	 */
	public void onConnect() {
		System.out.println("onConnect called");

		try {
			if (StatusCode.STATUS_CODE_SUCCESS != (lastSymbol.length() == 0 ? postFirstRequest() : postNextRequest(lastSymbol))) {
				getApplication().postDiesToThreads();
			}
		} catch (MiddlewareException e) {
			e.printStackTrace();
		}
	}

	/**
	 * invoked when asynchronous connect to the data source fails
	 */
	public void onConnectFailed(StatusCode statusCode) {
		System.out.println("onConnectFailed called, error - " + statusCode.toString());
		if (++reconnects > MAX_RECONNECTS) {
			getApplication().postDiesToThreads();
		}
	}

	/**
	 * invoked on unsolicited disconnect from the CG
	 */
	public void onBreak() {
		System.out.println("onBreak called");
	}

	// Post the first request.
	private StatusCode postFirstRequest() {

		if (requestBlockList.isEmpty()) {
			if (!fieldSpecificationList.isEmpty()) {
				Map<Character, Set<Integer>> relationIdToFieldIdSetMap = new HashMap<Character, Set<Integer>>();

				for (FieldSpecification fieldSpecification : fieldSpecificationList) {
					Set<Integer> fieldIdSet = relationIdToFieldIdSetMap.get(fieldSpecification.getRelationshipId());

					if (fieldIdSet == null) {
						fieldIdSet = new HashSet<Integer>();
						relationIdToFieldIdSetMap.put(fieldSpecification.getRelationshipId(), fieldIdSet);
					}
					fieldIdSet.add(fieldSpecification.getFieldId());
				}

				for (Character realationshipId : relationIdToFieldIdSetMap.keySet()) {
					RequestBlock requestBlock = new RequestBlock();

					requestBlock.relationshipId = realationshipId;
					requestBlock.fieldIdList.addAll(relationIdToFieldIdSetMap.get(realationshipId));

					requestBlockList.add(requestBlock);

					if (RelationshipIds.RELATIONSHIP_ID_NONE != requestBlock.relationshipId)
						isRelationshipIdNone = false;
				}
			} else {
				RequestBlock requestBlock = new RequestBlock();

				requestBlock.relationshipId = RelationshipIds.RELATIONSHIP_ID_NONE;
				requestBlock.flags |= RequestBlock.FLAG_ALL_FIELDS;

				requestBlockList.add(requestBlock);
			}
		}

		if (symbolListIterator.hasNext()) {
			StringBuilder symbolBuilder = new StringBuilder();

			String nextSymbol = symbolListIterator.next();

			for (int offset = 0; ((offset < nextSymbol.length()) && ('\\' != (nextSymbol.charAt(offset)) && ('*' != nextSymbol.charAt(offset)))); ++offset)
				symbolBuilder.append(nextSymbol.charAt(offset));

			symbol = symbolBuilder.toString();

			try {
				symbolPattern.initialize(nextSymbol);
			} catch (MiddlewareException e) {
				return e.getStatusCode();
			}
		} else {
			symbol = "";
			try {
				symbolPattern.initialize("*");
			} catch (MiddlewareException e) {
				return e.getStatusCode();
			}
			;
		}

		if (symbol.length() != 0) {
			GetEqual.RequestParameters requestParameters = new GetEqual.RequestParameters();

			requestParameters.symbolIdList.add(new SymbolId(config.getTableNumber(), this.symbol));
			requestParameters.permissionLevel = (config.isDelayed() ? PermissionLevel.PERMISSION_LEVEL_DELAYED : PermissionLevel.PERMISSION_LEVEL_DEFAULT);
			if (config.isIgnoreAlias()) requestParameters.flags |= GetEqual.RequestParameters.FLAG_DONT_RESOLVE_ALIAS;

			requestParameters.requestBlockList = requestBlockList;

			StatusCode statusCode = getEqual().postRequest(this, RequestId.generateEmptyRequestId(), requestParameters);
			if (StatusCode.STATUS_CODE_SUCCESS != statusCode)
				return statusCode;
		} else {
			GetFirst.RequestParameters requestParameters = new GetFirst.RequestParameters();

			requestParameters.tableNumber = config.getTableNumber();
			requestParameters.permissionLevel = (config.isDelayed() ? PermissionLevel.PERMISSION_LEVEL_DELAYED : PermissionLevel.PERMISSION_LEVEL_DEFAULT);
			if (config.isIgnoreAlias()) requestParameters.flags |= GetFirst.RequestParameters.FLAG_DONT_RESOLVE_ALIAS;

			requestParameters.requestBlockList = requestBlockList;

			StatusCode statusCode = getFirst().postRequest(this, RequestId.generateEmptyRequestId(), requestParameters);
			if (StatusCode.STATUS_CODE_SUCCESS != statusCode)
				return statusCode;
		}

		symbolToFieldKeyToValueMap.clear();

		return StatusCode.STATUS_CODE_SUCCESS;
	}

	private StatusCode postNextFirstRequest() {
		if (this.symbolListIterator.hasNext()) {
			if (StatusCode.STATUS_CODE_SUCCESS != postFirstRequest()) {
				getApplication().postDiesToThreads();
				//isSuccess = false;
			}
		} else {
			getApplication().postDiesToThreads();
			//isSuccess = true;
		}

		return StatusCode.STATUS_CODE_SUCCESS;
	}

	private StatusCode postNextRequest(String symbol) throws MiddlewareException {

		GetNext.RequestParameters requestParameters = new GetNext.RequestParameters();
		//System.out.println(" postNextRequest(String symbol)config.getTableNumber();\t" + config.getTableNumber());
		requestParameters.symbolId.tableNumber = config.getTableNumber();
		requestParameters.symbolId.symbol = symbol;
		requestParameters.permissionLevel = (config.isDelayed() ? PermissionLevel.PERMISSION_LEVEL_DELAYED : PermissionLevel.PERMISSION_LEVEL_DEFAULT);
		if (config.isIgnoreAlias()) requestParameters.flags |= GetNext.RequestParameters.FLAG_DONT_RESOLVE_ALIAS;
		requestParameters.numberOfRecords = 1204;

		requestParameters.requestBlockList = requestBlockList;

		StatusCode statusCode = getNext().postRequest(this, RequestId.generateEmptyRequestId(), requestParameters);
		if (StatusCode.STATUS_CODE_SUCCESS != statusCode)
			return statusCode;

		symbolToFieldKeyToValueMap.clear();

		return StatusCode.STATUS_CODE_SUCCESS;
	}

	@Override
	public void onGetFirstResponse(HeapMessage response) {

			if (isValidResponse(response)) {
			if ((!fieldSpecificationList.isEmpty()) && lastSymbol.length() == 0)
				outputHeader();

			GetFirst.ResponseParameters responseParameters = new GetFirst.ResponseParameters();

			if (StatusCode.STATUS_CODE_SUCCESS == getFirst().deserialize(this, response, responseParameters)) {
				processResponseBlockList(responseParameters.responseBlockList);

				if (isRelationshipIdNone)
					outputSymbols();
			}

			if (isCompleteResponse(response)) {
				outputSymbols();

				if ((this.symbol.length() == 0) || lastSymbol.equals(this.symbol)) {
					try {
						if (StatusCode.STATUS_CODE_SUCCESS != postNextRequest(this.lastSymbol)) {
							flushFile();
							getApplication().postDiesToThreads();
							//isSuccess = false;
						}
					} catch (MiddlewareException e) {
						e.printStackTrace();
					}
				} else {
					postNextFirstRequest();
				}
			}
		} else if (StatusCode.STATUS_CODE_NOT_FOUND == response.getStatusCode()) {
			postNextFirstRequest();
		} else if (StatusCode.STATUS_CODE_NOT_CONNECTED != response.getStatusCode()) {
			flushFile();
			getApplication().postDiesToThreads();
		}
	}

	private void flushFile() {
		if (file != null) {
			try {
				file.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void onGetNextResponse(HeapMessage response) {


		if (isValidResponse(response)) {
			GetNext.ResponseParameters responseParameters = new GetNext.ResponseParameters();

			if (StatusCode.STATUS_CODE_SUCCESS == getNext().deserialize(this, response, responseParameters)) {
				processResponseBlockList(responseParameters.responseBlockList);

				if (isRelationshipIdNone)
					outputSymbols();
			}

			if (isCompleteResponse(response)) {
				outputSymbols();

				if (this.symbol.length() == 0 || (this.lastSymbol.startsWith(this.symbol))) {
					try {
						if (StatusCode.STATUS_CODE_SUCCESS != postNextRequest(this.lastSymbol)) {
							flushFile();
							getApplication().postDiesToThreads();
							//isSuccess = false;
						}
					} catch (MiddlewareException e) {
						e.printStackTrace();
					}
				} else {
					postNextFirstRequest();
				}
			}
		} else if (StatusCode.STATUS_CODE_NOT_CONNECTED != response.getStatusCode()) {
			flushFile();
			getApplication().postDiesToThreads();
		}
	}

	public void onGetEqualResponse(HeapMessage response) {

		if (isValidResponse(response)) {
			if ((!fieldSpecificationList.isEmpty()) && lastSymbol.length() == 0)
				outputHeader();

			this.lastSymbol = this.symbol;

			GetEqual.ResponseParameters responseParameters = new GetEqual.ResponseParameters();

			if (StatusCode.STATUS_CODE_SUCCESS == getEqual().deserialize(this, response, responseParameters)) {
				processResponseBlockList(responseParameters.responseBlockList);

				if (isRelationshipIdNone)
					outputSymbols();
			}

			if (isCompleteResponse(response)) {
				outputSymbols();

				try {
					if (StatusCode.STATUS_CODE_SUCCESS != postNextRequest(this.lastSymbol)) {
						flushFile();
						getApplication().postDiesToThreads();
						//isSuccess = false;
					}
				} catch (MiddlewareException e) {
					e.printStackTrace();
				}
			}
		} else if (StatusCode.STATUS_CODE_NOT_CONNECTED != response.getStatusCode()) {
			flushFile();
			getApplication().postDiesToThreads();
		}
	}

	private void processResponseBlockList(List<ResponseBlock> responseBlockList) {

		for (ResponseBlock responseBlock : responseBlockList) {
			Map<FieldSpecification, String> fieldKeyToValueMap = symbolToFieldKeyToValueMap.get(responseBlock.resolvedKey.symbol);
			if (fieldKeyToValueMap == null) {
				fieldKeyToValueMap = new HashMap<FieldSpecification, String>();
				symbolToFieldKeyToValueMap.put(responseBlock.resolvedKey.symbol, fieldKeyToValueMap);
			}

			// When a symbol is specified the first request is a GetEqual, so if the symbol is not matched in the specified table it
			// will return the symbol from another table (where possible). Here this is not the desired behavior: we want to walk the
			// specified table, or records with relationships to this table. So if a response comes from another table without being
			// caused by a relationship, we ignore it.
			if (this.symbol.length() != 0 &&
					(responseBlock.responseKey.tableNumber != this.config.getTableNumber())
					&& (RelationshipIds.RELATIONSHIP_ID_NONE == responseBlock.relationshipId)) {
				continue;
			}

			if (responseBlock.isValidResponse()) {

				try {
					this.fieldListValidator.initialize(responseBlock.fieldData);

					if (this.symbol.length() == 0 || (0 == responseBlock.resolvedKey.symbol.indexOf(symbol))) {
						for (FieldListValidator.Field field : fieldListValidator) {
							fieldKeyToValueMap.put(new FieldSpecification(responseBlock.relationshipId, field.fieldId), fieldToString(field));
						}
					}
				} catch (MiddlewareException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void outputHeader() {
		StringBuilder output = new StringBuilder();

		try {
			for (FieldSpecification fieldSpecification : fieldSpecificationList) {
				if (output.length() != 0)
					output.append(separator);

				output.append((((!isRelationshipIdNone) ? (Integer.toString(fieldSpecification.getRelationshipId()) + ":") : "") + fieldSpecification.getName()));
			}

			if (file != null) {
				output.append("\n");
				file.write(output.toString().getBytes());
			} else {
			//	System.out.println(output);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void outputSymbols() {
		try {
			if (fieldSpecificationList.isEmpty()) {
				for (FieldListValidator.Field field : fieldListValidator) {

					final FieldSpecification fieldSpecification = new FieldSpecification(RelationshipIds.RELATIONSHIP_ID_NONE, field.fieldId);
					if (ignoreFieldSpecificationList.size() == 0 || !ignoreFieldSpecificationList.contains(fieldSpecification)) {
						if (fieldSpecification.getName().equalsIgnoreCase("LastUpdateDate")) {
							fieldSpecificationList.add(fieldSpecification);
						}
						if (fieldSpecification.getName().equalsIgnoreCase("LocalCode")){
							fieldSpecificationList.add(fieldSpecification);
						}
						if (fieldSpecification.getName().equalsIgnoreCase("Symbol")){
							fieldSpecificationList.add(fieldSpecification);
						}

					}
				}

				//outputHeader1();
			}

			StringBuilder output = new StringBuilder();

			for (String symbol : symbolToFieldKeyToValueMap.keySet()) {
				if (!this.symbolPattern.isMatch(symbol, 0)) {
					this.lastSymbol = symbol;
					continue;
				}
				Map<FieldSpecification, String> fieldKeyToValueMap = symbolToFieldKeyToValueMap.get(symbol);

				if (!fieldKeyToValueMap.isEmpty()) {
					boolean firstField = true;
					for (FieldSpecification fieldSpecification : fieldSpecificationList) {
						if (!firstField)
							output.append(separator);

						firstField = false;

						if (fieldSpecification.getName().equalsIgnoreCase("LastUpdateDate")){
							output.append("META");
						}
						if (fieldSpecification.getName().equalsIgnoreCase("LocalCode")){
							String value = fieldKeyToValueMap.get(new FieldSpecification(fieldSpecification.getRelationshipId(), 301));
							if (value != null){
								// Call the replaceAll() method
								value = value.replaceAll("\\s", "");

								output.append(value);

							}
						}
						if (fieldSpecification.getName().equalsIgnoreCase("Symbol")){
							String value = fieldKeyToValueMap.get(new FieldSpecification(fieldSpecification.getRelationshipId(), 456));
							if (value != null){

								output.append(value);


							}
						}

					}
					output.append("\n");

				}

				this.lastSymbol = symbol;
			}

			if (file != null) {
				file.write(output.toString().getBytes());
			} else {

		//	System.out.println(output);


			}

			symbolToFieldKeyToValueMap.clear();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String fieldToString(FieldListValidator.Field field) {


		switch (field.fieldStatus) {
			case FieldStatus.FIELD_STATUS_DEFINED: {
				if (FieldTypeConsts.FIELD_TYPE_TIME == field.fieldType.getType())
					return timeToString(field);
				else if ((FieldTypeConsts.FIELD_TYPE_TRATIONAL != field.fieldType.getType()) || (config.isExtendedTRationalDisplay()))
					return field.fieldType.toString();
				else
					return ((TRational) (field.fieldType)).getRational().toString();
			}

			case FieldStatus.FIELD_STATUS_NOT_PERMISSIONED:
				return "???";

			case FieldStatus.FIELD_STATUS_UNDEFINED:
				return "---";

			default:
				return "";
		}
	}

	private String timeToString(FieldListValidator.Field field) {
		try {
			switch (config.getFieldTimestampFormat()) {
				case TIME_FORMAT_SECONDS:
				case TIME_FORMAT_MILLISECONDS:
				case TIME_FORMAT_MICROSECONDS:
				case TIME_FORMAT_NANOSECONDS: {
					final Resolution resolution = getTimeResolution(config.getFieldTimestampFormat());
					return ((Time) (field.fieldType)).toString(resolution);
				}

				case TIME_FORMAT_RECIEVED:
					return ((Time) (field.fieldType)).toString();

				case TIME_FORMAT_SMART: {
					final Resolution resolution = ((Time) (field.fieldType)).getResolution();

					Resolution bestResolution = config.getBestResoltion();

					final int isHigherResolution = (bestResolution != null) ? resolution.compareTo(bestResolution) : 0;

					if (isHigherResolution >= 0)
						bestResolution = resolution;

					return ((Time) (field.fieldType)).toString(bestResolution);
				}

				default:
					throw new MiddlewareException(StatusCode.STATUS_CODE_INVALID_PARAMETER);
			}
		} catch (MiddlewareException e) {
			System.out.println(e.toString());
		}

		return "";
	}

	private Resolution getTimeResolution(ProgramConfiguration.TimestampFormat timestampFormat) throws MiddlewareException {
		switch (timestampFormat) {
			case TIME_FORMAT_SECONDS:
				return Resolution.RESOLUTION_SECOND;

			case TIME_FORMAT_MILLISECONDS:
				return Resolution.RESOLUTION_MILLISECOND;

			case TIME_FORMAT_MICROSECONDS:
				return Resolution.RESOLUTION_MICROSECOND;

			case TIME_FORMAT_NANOSECONDS:
				return Resolution.RESOLUTION_NANOSECOND;

			default:
				throw new MiddlewareException(StatusCode.STATUS_CODE_INVALID_PARAMETER);
		}
	}
}
