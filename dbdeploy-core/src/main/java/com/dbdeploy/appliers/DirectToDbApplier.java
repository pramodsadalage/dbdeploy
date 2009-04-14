package com.dbdeploy.appliers;

import com.dbdeploy.database.changelog.DatabaseSchemaVersionManager;
import com.dbdeploy.database.changelog.QueryExecuter;
import com.dbdeploy.scripts.ChangeScript;

import java.sql.SQLException;

public class DirectToDbApplier extends AbstractChangeScriptApplier {
	private final QueryExecuter queryExecuter;
	private final DatabaseSchemaVersionManager schemaVersionManager;

	public DirectToDbApplier(QueryExecuter queryExecuter, DatabaseSchemaVersionManager databaseSchemaVersion) {
		super(ApplyMode.DO);
		this.queryExecuter = queryExecuter;
		schemaVersionManager = databaseSchemaVersion;
	}

	@Override
	public void begin() {
		try {
			queryExecuter.setAutoCommit(false);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void preChangeScriptApply(ChangeScript changeScript) {
		System.out.println("Applying " + changeScript + "...");
	}

	@Override
	protected void beginTransaction() {
		// no need to explictly begin, autoCommit mode has been disabled
	}

	@Override
	protected void applyChangeScriptContent(String scriptContent) {
		try {
			queryExecuter.execute(scriptContent);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void insertToSchemaVersionTable(ChangeScript changeScript) {
		String sql = schemaVersionManager.getChangelogInsertSql(changeScript);
		try {
			queryExecuter.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void deleteFromSchemaVersionTable(ChangeScript changeScript) {
	}

	@Override
	protected void commitTransaction() {
		try {
			queryExecuter.commit();
		} catch (SQLException e) {
			throw new RuntimeException();
		}
	}

	@Override
	protected void postChangeScriptApply(ChangeScript changeScript) {
	}


	@Override
	public void end() {
	}
}
