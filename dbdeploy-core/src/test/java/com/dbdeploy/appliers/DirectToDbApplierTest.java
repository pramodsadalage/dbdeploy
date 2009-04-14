package com.dbdeploy.appliers;

import com.dbdeploy.database.changelog.QueryExecuter;
import com.dbdeploy.database.changelog.DatabaseSchemaVersionManager;
import com.dbdeploy.scripts.ChangeScript;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnit44Runner;

@RunWith(MockitoJUnit44Runner.class)
public class DirectToDbApplierTest {
	@Mock private QueryExecuter queryExecuter;
	@Mock private DatabaseSchemaVersionManager schemaVersionManager;
	private DirectToDbApplier applier;

	@Before
	public void setUp() {
		applier = new DirectToDbApplier(queryExecuter, schemaVersionManager);
	}
	
	@Test
	public void shouldSetConnectionToManualCommitModeAtStart() throws Exception {
		applier.begin();

		verify(queryExecuter).setAutoCommit(false);
	}

	@Test
	public void shouldUpdateApplyScriptInANoddyWayThatWillFailOnNonTrivialExamples() throws Exception {
		applier.applyChangeScriptContent("content");
		
		verify(queryExecuter).execute("content");
	}

	@Test
	public void shouldInsertToSchemaVersionTable() throws Exception {
		ChangeScript changeScript = new ChangeScript(1, "script.sql");

		when(schemaVersionManager.getChangelogInsertSql(changeScript)).thenReturn("the insert script");

		applier.insertToSchemaVersionTable(changeScript);

		verify(queryExecuter).execute("the insert script");
	}

	@Test
	public void shouldCommitTransactionOnErrrCommitTransaction() throws Exception {
		applier.commitTransaction();

		verify(queryExecuter).commit();
	}


}
