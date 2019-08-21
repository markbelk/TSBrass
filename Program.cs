using Integration_v2;
using Integration_v2.SfdcPartnerApi;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Xml;

namespace TransferTSBrassSalesforce
{
    class Program : DirectDatabaseTransfer
    {
        public static string syteLineDbConnection = ConfigurationManager.ConnectionStrings["SytelineDbConn"].ConnectionString;
        public static string siteName = ConfigurationManager.AppSettings["SiteName"];

        public Program(SalesforceLogin sfLogin, SupportEmail supportEmail, int appId, string stagingDbConnString) :
            base(sfLogin, supportEmail, appId, stagingDbConnString)
        { }
        static void Main(string[] args)
        {
            string sfUserName = ConfigurationManager.AppSettings["SalesForceUsername"];
            string sfPassword = ConfigurationManager.AppSettings["SalesForcePassword"];
            string securityToken = ConfigurationManager.AppSettings["SalesForceSecurityToken"];
            int appId = Convert.ToInt16(args[0]);
            string connString = ConfigurationManager.ConnectionStrings["ITCStagingDbConn"].ConnectionString;
            string fromEmail = ConfigurationManager.AppSettings["fromEmail"]; ;
            string emailPassword = ConfigurationManager.AppSettings["emailPassword"];
            string toEmails = ConfigurationManager.AppSettings["toEmail"];

            try
            {
                Transfer.SalesforceLogin login = new Transfer.SalesforceLogin(
                    sfUserName,
                    sfPassword,
                    securityToken
                    );
                Transfer.SupportEmail supportEmail = new Transfer.SupportEmail(fromEmail, toEmails.Split(new char[] { ',' }).ToList(), Transfer.DecryptString(emailPassword));               
                Program p = new Program(login, supportEmail, appId, connString);
                p.interfacedSystemConnString = syteLineDbConnection;           
                if (args.Length > 1) { p.EmailAppSummaryRpt = Convert.ToBoolean(args[1]); }
                p.Run();
            }

            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.ToString());
            }

            if (Convert.ToBoolean(ConfigurationManager.AppSettings["CloseConsoleOnCompletion"]) == false)
            {
                Console.Read();
            }
        }

        public override void DoExport()
        {
            int rowsAffected = 0;
            try
            {
                Process.FieldMappings = GetFieldMappings();

                if (!LoggedIntoSalesforce)
                {
                    //authenticate to Salesforce
                    AuthenticateToSalesforce(sfLogin.Username, sfLogin.Password, sfLogin.SecurityToken);
                    LoggedIntoSalesforce = true;
                }

                /*
                //Build Soql Query Filter             
                DateTime targetDate = LastRunDate.ToUniversalTime();
                StringBuilder sbFilter = new StringBuilder();
                if (Process.ToObjectName == "Prospect")
                {
                    // Prod - 
                    sbFilter.AppendFormat("Where LastModifiedDate >= {0:yyyy-MM-ddThh:mm:ss.sssZ} And RecordTypeId='01241000001UVF8AAO' AND IsDeleted=false", targetDate);
                    // Sandbox - sbFilter.AppendFormat("Where LastModifiedDate >= {0:yyyy-MM-ddThh:mm:ss.sssZ} And RecordTypeId='0125C000000IwFHQA0' AND IsDeleted=false", targetDate);				
                }
                else if (Process.ToObjectName == "Syteline Prospect Contact X Ref")
                {
                    sbFilter.AppendFormat("Where LastModifiedDate >= {0:yyyy-MM-ddThh:mm:ss.sssZ} AND IsDeleted=false", targetDate);
                    sbFilter.Append(" AND Prospect_Contact_Row_Pointer__c = NULL");
                    sbFilter.Append(" AND Account.Syteline_Prospect_Id__c != NULL");
                }
                else if (Process.ToObjectName == "Syteline Customer Contact X Ref")
                {
                    sbFilter.AppendFormat("Where LastModifiedDate >= {0:yyyy-MM-ddThh:mm:ss.sssZ} AND IsDeleted=false", targetDate);
                    sbFilter.Append(" AND Customer_Contact_Row_Pointer__c = NULL");
                    sbFilter.Append(" AND Account.Cust_Num__c != NULL");
                }
                else
                {
                    sbFilter.AppendFormat("Where LastModifiedDate >= {0:yyyy-MM-ddThh:mm:ss.sssZ} AND IsDeleted=false", targetDate);
                }

                */

                string soqlQueryFilter = BuildSoqlQueryFilter();

                String soqlQuery = createSoqlQuery(Process.FromObjectName, SalesforceUrl, Process.FieldMappings, soqlQueryFilter);


                //Get Query Results
                sObject[] sObjects = Proxy.getQueryResults(soqlQuery, SalesforceSessionId, SalesforceUrl);   
               

                if (Process.ToObjectName == "Prospect")
                {
                    rowsAffected = processProspects(sObjects);
                }

                else if (Process.ToObjectName == "Syteline Contact")
                {
                    rowsAffected = processContacts(sObjects);
                }

                else if (Process.ToObjectName == "Syteline Prospect Contact X Ref")
                {
                    rowsAffected = processProspectContactXRef(sObjects);
                }

                else if (Process.ToObjectName == "Syteline Customer Contact X Ref")
                {                  
                    rowsAffected = processCustomerContactXRef(sObjects);                    
                }

                else if (Process.ToObjectName == "Interaction")
                {
                    rowsAffected = processCustInteractions(sObjects);
                }

                else if (Process.ToObjectName == "Interaction Detail")
                {
                    rowsAffected = processConversations(sObjects);
                }
             
            }
            catch (Exception ex)
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("Error during sync process:{0} - ", Process.ID, ex.StackTrace);
                CreateJobLogSyncProcessEntry(Process.ID, sb.ToString(), 3);
            }
            finally
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("There were {0} rows processed for table {1}", rowsAffected, Process.ToObjectName);
                CreateJobLogSyncProcessEntry(Process.ID, sb.ToString(), 2);

                //check for row level error(s)
                if (Process.EmailErrorReport)
                {
                    DataSet dsErrors = GetSFToSytelineSyncErrors();
                    //send an email with error report attached	
                    if (dsErrors.Tables[0].Rows.Count > 0)
                    {
                        SendErrorEmail(supportEmail, dsErrors.Tables[0], Process.ToObjectName, JobId);
                    }
                }
                if (Process.EmailSuccessReport)
                {
                    //DataSet dsSuccess = GetSuccessReport(Process.SuccessReportSprocName);
                    //SendSuccessEmail(supportEmail, dsSuccess.Tables[0], Process.ToObjectName, JobId);
                }
            }
        }

        public override void UpsertISData()
        {         
            int rowsProcessed = 0;
            string externalId = "";
            //string externalId = GetUniqueIdField(Process.FieldMappings);

            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();

            while (Process.RowCount > rowsProcessed)
            {
                //Process data rows for current object
                stopwatch.Start();
                DataTable isData = GetISData(Process.SourceTableName).Tables[0];

                //prepare IS data using field mappings
                sObject[] sObjectArray = PrepareISData(isData, Process.FieldMappingsDict, Process.ToObjectName);

                if (externalId == "") { externalId = GetUniqueIdField(Process.FieldMappingsDict, isData); }

                //call SfApiProxy.upsertToSalesforce function
                UpsertResult[] ur = Proxy.upsertToSalesforce(sObjectArray, externalId);

                //log results to the sync Process detail table
                CreateJobLogSyncProcessDetailEntries(isData, ur, Process.ID);

                //increment rows processed counter
                rowsProcessed += isData.Rows.Count;
            }

            stopwatch.Stop();
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("There were {0} rows processed for table {1}", rowsProcessed, Process.ToObjectName);
            //assign job status based on results of the above sproc
            CreateJobLogSyncProcessEntry(Process.ID, sb.ToString(), 2, stopwatch.Elapsed);
        }

        public override string GetUniqueIdField(Dictionary<string, List<FieldMapping>> fieldMappings, DataTable dt)
        {
            string fieldName = "Id";
            if (dt.Columns.Contains("Uf_SFUID"))
            {
                if (!string.IsNullOrEmpty(dt.Rows[0]["Uf_SFUID"].ToString()))
                {
                    return fieldName;
                }

            }
            if (fieldMappings.Count > 0)
            {
                foreach (KeyValuePair<string, List<FieldMapping>> kvp in fieldMappings)
                {
                    foreach (FieldMapping mapping in fieldMappings[kvp.Key])
                        if (mapping.IsUnique)
                        {
                            if (dt.Columns.Contains(mapping.FromField))
                            {
                                foreach (DataRow dr in dt.Rows)
                                {
                                    if (!string.IsNullOrEmpty(dr[mapping.FromField].ToString()))
                                    {
                                        return mapping.ToField;
                                    }
                                }
                            }
                        }
                }
            }
            return fieldName;

        }

        private int processProspects(sObject[] sObjects)
        {
            int rowsAffected = 0;
            foreach (sObject so in sObjects)
            {
                foreach (XmlElement el in so.Any)
                {
                    if (el.LocalName == "Syteline_Prospect_Id__c")
                    {
                        if (el.InnerText != "")
                        {
                            //call logic for update							
                            string updateStmt = createUpdateStmt(so, Process.FieldMappings, "[prospect]", "pro", "Prospects");
                            rowsAffected += doSqlUpdate(updateStmt, so.Id);
                        }
                        else
                        {
                            //call logic for insert							
                            //string insertStmt = createProspectInsertStmt(so.Any, so.Id);
                            string insertStmt = createProspectInsertStmt(so, Process.FieldMappings, Process.DestTableName, "Prospects");
                            rowsAffected += doSqlInsert(insertStmt, so.Id);
                        }
                    }
                }
            }
            return rowsAffected;
        }

        private int processContacts(sObject[] sObjects)
        {
            int rowsAffected = 0;
            foreach (sObject so in sObjects)
            {
                //don't process any Contacts before the Syteline prospect Id has been saved
                //back to the related Account in Salesforce
                if (!shouldProcessContact(so.Any)) { continue; }
                if (isContactUpdate(so.Any))
                {
                    //unmap the syteline contact id before update
                    unmapSytelineContactId();
                    //call logic for update
                    //string updateStmt = createContactUpdateStmt(so.Any);
                    string updateStmt = createUpdateStmt(so, Process.FieldMappings, "[contact]", "cont", "SalesContacts");
                    rowsAffected += doSqlUpdate(updateStmt, so.Id);
                }
                else
                {
                    //call logic for insert
                    Guid guid = Guid.NewGuid();
                    //initSession(guid);
                    //string insertStmt = createContactInsertStmt(so.Any, guid, so.Id);
                    string insertStmt = createContactInsertStmt(so, Process.FieldMappings, Process.DestTableName, "SalesContacts");
                    //initSubSession("SalesContacts");
                    rowsAffected += doSqlInsert(insertStmt, so.Id);
                }
            }
            return rowsAffected;
        }
        private int processProspectContactXRef(sObject[] sObjects)
        {
            int rowsAffected = 0;
            foreach (sObject so in sObjects)
            {
                if (Process.InterfacedSystemActionType.ToLower() == "update")
                {
                    //ToDo: test this function                  
                    string updateStmt = createProspectContactXrefUpdateStmt("[TR_App].[dbo].[prospect_contact_mst]", so.Id, getValueByName(so.Any, "Prospect__c"), getValueByName(so.Any, "Contact__c"));
                    rowsAffected += doSqlUpdate(updateStmt, so.Id);
                }
                else
                {
                    string insertStmt = createProspectContactInsertStmt(so.Any);
                    rowsAffected += doSqlInsert(insertStmt, so.Id);
                }
            }
            return rowsAffected;
        }

        private int processCustomerContactXRef(sObject[] sObjects)
        {
            int rowsAffected = 0;
            foreach (sObject so in sObjects)
            {               
                if (Process.InterfacedSystemActionType.ToLower() == "update")
                {
                    //string updateStmt = createUpdateStmt(so, Process.FieldMappings, "[customer_contact]", "custcont", "ProspectSalesContactCrossReferences");
                    string updateStmt = createCustContactXrefUpdateStmt("[TR_App].[dbo].[customer_contact_mst]", so.Id, getValueByName(so.Any, "Cust_Num__c"), getValueByName(so.Any, "Contact__c"));
                    rowsAffected += doSqlUpdate(updateStmt, so.Id);
                }
                else
                {
                    string insertStmt = createCustomerContactInsertStmt(so.Any);
                    rowsAffected += doSqlInsert(insertStmt, so.Id);
                }
           
            }
            return rowsAffected;
        }

        private string getValueByName(XmlElement [] elements, string targetElementName)
        {         
            foreach(XmlElement el in elements)
            {
                if (el.LocalName == "Account" || el.LocalName=="Contact")
                {
                    foreach (XmlElement childElement in el)
                    {
                        if (childElement.LocalName == targetElementName)
                        {
                            return childElement.InnerText;
                        }
                    }
                }
                else if (el.LocalName == targetElementName)
                    {
                        return el.InnerText;
                    }
                
            }
            throw new Exception("target element not found: " + targetElementName);
        } 
        private int processCustInteractions(sObject[] sObjects)
        {
            int rowsAffected = 0;
            foreach (sObject so in sObjects)
            {
                if (!shouldProcessInteraction(so.Any)) { continue; }
                if (isInteractionUpdate(so.Any))
                {//ToDo:add update logic if we decide to implement it}
                }
                else
                {//do interaction insert
                    string insertStmt = createInsertStmt(so, Process.FieldMappings, Process.DestTableName, "CustomerInteractions");
                    rowsAffected += doSqlInsert(insertStmt, so.Id);
                }

            }
            return rowsAffected;
        }
        public int processConversations(sObject[] sObjects)
        {
            int rowsAffected = 0;
            foreach (sObject so in sObjects)
            {
                if (!shouldProcessConversation(so.Any)) { continue; }
                if (isConversationUpdate(so.Any))
                {
                    //ToDo: add update logic if we decide to implement it
                }
                else
                {
                    //insert the Conversation in Syteline
                    string insertStmt = createInsertStmt(so, Process.FieldMappings, Process.DestTableName, "CustomerInteractions");
                    rowsAffected += doSqlInsert(insertStmt, so.Id);
                }
            }
            return rowsAffected;
        }

   
        private string createContactInsertStmt(sObject sObject, List<FieldMapping> fieldMappings, string destTableName, string contextName)
        {

            StringBuilder sb = new StringBuilder();
            sb.Append(createInitSubSession(contextName));
            sb.AppendFormat("exec sp_executesql N'INSERT {0}", destTableName);
            sb.AppendFormat("({0})", buildColumnsList(fieldMappings, sObject) + ",[contact_id]");
            sb.AppendFormat("VALUES ({0})", buildValuesList(fieldMappings, sObject) + ",@ContactID");
            sb.AppendFormat("SELECT @@ROWCOUNT',N'{0}'", buildParameterList(fieldMappings, sObject) + ",@ContactID nvarchar(3)");
            string argumentList = buildArgumentList(fieldMappings, sObject, false) + ",@ContactID=N'TBD'";
            argumentList = argumentList.Replace("false", "0").Replace("true", "1");
            sb.AppendFormat(",{0}", argumentList);         
            return sb.ToString();
        }

        public string createProspectContactInsertStmt(XmlElement[] elements)
        {            
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("exec InitSession2SubSp @ID='{0}',@UserName=NULL,@ConfigName=NULL,@FrameworkSession=0,@Site=N'TR',@ContextName=N'ProspectSalesContactCrossReferences' ", Guid.NewGuid());
            sb.Append("exec sp_executesql N'INSERT [prospect_contact] ([contact_id], [prospect_id], [InWorkflow],[RowPointer],[Uf_SFUID]) ");
            sb.Append("VALUES (@ContactID, @ProspectID, @InWorkflow, @RowPointer,@Uf_SFUID)");
            sb.Append("SELECT @@ROWCOUNT',N'@ContactID nvarchar(7),@ProspectID nvarchar(7),@InWorkflow tinyint,@RowPointer nvarchar(36),@Uf_SFUID nvarchar(18)'");        
            foreach (XmlElement el in elements)
            {
                if (el.LocalName == "Contact") {
                    //sb.AppendFormat(",@ContactID=N'{0}'", el.InnerText.PadLeft(7));
                    foreach (XmlElement childElement in el.ChildNodes)
                    {
                        if (childElement.LocalName == "Contact__c")
                        {
                            sb.AppendFormat(",@ContactID=N'{0}'", childElement.InnerText.PadLeft(7));
                        }
                    }
                }
                if (el.LocalName == "Account")
                {
                    foreach (XmlElement childElement in el.ChildNodes)
                    {
                        if (childElement.LocalName == "Prospect__c")
                        {
                            sb.AppendFormat(",@ProspectID=N'{0}'", childElement.InnerText.PadLeft(7));
                        }
                    }
                }
                if (el.LocalName == "Id")
                {
                    sb.AppendFormat(",@Uf_SFUID=N'{0}'", el.InnerText);
                }

            }
            sb.Append(",@InWorkflow=0");
            sb.AppendFormat(",@RowPointer=N'{0}'", Guid.NewGuid());        
            return sb.ToString();
        }

        public string createCustomerContactInsertStmt(XmlElement[] elements)
        {          
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("exec InitSession2SubSp @ID='{0}',@UserName=NULL,@ConfigName=NULL,@FrameworkSession=0,@Site=N'TR',@ContextName=N'CustomerSalesContactCrossReferences' ", Guid.NewGuid());
            sb.Append("exec sp_executesql N'INSERT [customer_contact] ([contact_id], [cust_num], [InWorkflow], [RowPointer], [cust_seq], [Uf_SFUID]) ");
            sb.Append("VALUES (@ContactID, @CustNum, @InWorkflow, @RowPointer, @CustSeq, @Uf_SFUID)");
            sb.Append("SELECT @@ROWCOUNT',N'@ContactID nvarchar(7),@CustNum nvarchar(7),@InWorkflow tinyint,@RowPointer nvarchar(36), @CustSeq int, @Uf_SFUID nvarchar(18)'");
            foreach (XmlElement el in elements)
            {

                if (el.LocalName == "Contact")
                {
                    foreach (XmlElement childElement in el.ChildNodes)
                    {
                        if (childElement.LocalName == "Contact__c") { sb.AppendFormat(",@ContactID=N'{0}'", childElement.InnerText.PadLeft(7)); }
                    }
                }
                if (el.LocalName == "Account")
                {
                    foreach (XmlElement childElement in el.ChildNodes)
                    {
                        if (childElement.LocalName == "Cust_Num__c") { sb.AppendFormat(",@CustNum=N'{0}'", childElement.InnerText); }
                    }
                }
                if (el.LocalName == "Id")
                {
                    sb.AppendFormat(",@Uf_SFUID=N'{0}'", el.InnerText);
                }
            }
            sb.Append(",@InWorkflow=0");
            sb.AppendFormat(",@RowPointer=N'{0}'", Guid.NewGuid());
            sb.Append(",@CustSeq=0");
            return sb.ToString();
        }     
        public string createProspectInsertStmt(sObject sObject, List<FieldMapping> fieldMappings, string destTableName, string contextName)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(createInitSubSession(contextName));
            sb.AppendFormat("exec sp_executesql N'INSERT {0}", destTableName);
            sb.AppendFormat("({0})", buildColumnsList(fieldMappings, sObject) + ",[prospect_id]");
            sb.AppendFormat("VALUES ({0})", buildValuesList(fieldMappings, sObject) + ",@ProspectID");
            sb.AppendFormat("SELECT @@ROWCOUNT',N'{0}'", buildParameterList(fieldMappings, sObject) + ",@ProspectID nvarchar(3)");
            sb.AppendFormat(",{0}", buildArgumentList(fieldMappings, sObject, false) + ",@ProspectID=N'TBD'");
            return sb.ToString();   
        }

        private void initSession(Guid guid)
        {
            DataSet dsResults = new DataSet();
            try
            {
                using (SqlConnection connection = new SqlConnection(syteLineDbConnection))
                {
                    connection.Open();
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = "[dbo].[InitSessionContextSp]";
                        command.Parameters.AddWithValue("@SessionId", guid);                      
                        command.Parameters.AddWithValue("@Site", siteName);
                        command.Parameters.AddWithValue("@ContextName", "Sessionless Connection");
                        command.CommandTimeout = 120;

                        new SqlDataAdapter(command).Fill(dsResults);
                    }
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception: " + ex.Message);
                CreateJobLogSyncProcessEntry(Process.ID, ex.Message, 3);
            }
        }

        private void initSubSession(string contextName)
        {
            DataSet dsResults = new DataSet();
            try
            {
                using (SqlConnection connection = new SqlConnection(syteLineDbConnection))
                {
                    connection.Open();
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = "[dbo].[InitSession2SubSp]";
                        command.Parameters.AddWithValue("@ID", Guid.NewGuid());
                        command.Parameters.AddWithValue("@UserName", "NULL");
                        command.Parameters.AddWithValue("@ConfigName", "NULL");
                        command.Parameters.AddWithValue("@FrameworkSession", 0);
                        command.Parameters.AddWithValue("@Site", siteName);
                        command.Parameters.AddWithValue("@ContextName", contextName);
                        command.CommandTimeout = 120;

                        new SqlDataAdapter(command).Fill(dsResults);
                    }
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception: " + ex.Message);
                CreateJobLogSyncProcessEntry(Process.ID, ex.Message, 3);
            }
        }


        public int doSqlInsert(string insertStmt, string sfID)
        {
            int rowsAffected = 0;
            try
            {
                using (SqlConnection connection = new SqlConnection(interfacedSystemConnString))
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand(insertStmt, connection);
                    rowsAffected = command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                StringBuilder sb = new StringBuilder();
                bool isError = false;                
                if (ex.Message != "<MsgTag>Your session has been deleted.  You must close your session and log on again to continue.<MsgTag>")
                {
                    sb.AppendFormat("{0}", ex.Message);
                    isError = true;          
                }
                else
                {
                    rowsAffected = 1;
                    sb.Insert(0, "Record successfully created. ");
                }
                sb.Replace("<MsgTag>Your session has been deleted.  You must close your session and log on again to continue.<MsgTag>", "");
                Console.WriteLine(sb.ToString());
                CreateJobLogSyncProcessDetailEntry(sb.ToString(), isError, Process.ID, sfID);
            }
            return rowsAffected;
        }

        public int doSqlUpdate(string updateStmt, string sfID)
        {
            int rowsAffected = 0;
            bool isError = false;
            StringBuilder sb = new StringBuilder();
            try
            {
                using (SqlConnection connection = new SqlConnection(interfacedSystemConnString))
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand(updateStmt, connection);
                    rowsAffected = command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {                
                sb.AppendFormat("Exception: {0}", ex.Message);
                if (ex.Message != "<MsgTag>Your session has been deleted.  You must close your session and log on again to continue.<MsgTag>")
                {
                    sb.Replace("<MsgTag>Your session has been deleted.  You must close your session and log on again to continue.<MsgTag>", "");
                    isError = true;
                }
                else//handle false positive scenario
                {
                    rowsAffected = 1;
                    sb.Clear();
                    sb.Append(" Record successfully updated.");
                }
                Console.WriteLine(sb.ToString());
                CreateJobLogSyncProcessDetailEntry(sb.ToString(), isError, Process.ID, sfID);
                return rowsAffected;
            }

            //handle non-error scenarios
            if (rowsAffected == 0)
            {
                sb.Append("No matching Syteline record found.");
            }
            else if(rowsAffected == 1)
            {
                sb.Append("Record successfully updated.");
            }
            Console.WriteLine(sb.ToString());
            CreateJobLogSyncProcessDetailEntry(sb.ToString(), isError, Process.ID, sfID);
            return rowsAffected;
        }

        public string GetUniqueIdField(List<FieldMapping> mappings, DataTable isData)
        {
            string fieldName = "Id";
            //if the row contatins a Salesforce Id use it as the external id
            foreach (FieldMapping mapping in mappings)
            {
                if (mapping.ToField == "Id")
                {
                    foreach (DataRow dr in isData.Rows)
                    {
                        if (!DBNull.Value.Equals(dr[mapping.FromField]))
                        {
                            return fieldName;
                        }
                    }
                }
            }

            //otherwise use the external Id specified in the field mappings
            foreach (FieldMapping mapping in mappings)
            {
                if (mapping.IsUnique)
                {
                    fieldName = mapping.ToField;
                }
            }
            return fieldName;

        }

        /// <summary>
        /// Checks that a relationship has been established with
        /// either Syteline Customer or Prospect
        /// </summary>
        /// <param name="xmlElements"></param>
        /// <returns></returns>
        public bool shouldProcessContact(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (el.LocalName == "Account")
                {
                    foreach (XmlElement childElement in el)
                    {
                        if (childElement.LocalName == "Syteline_Prospect_Id__c" || childElement.LocalName=="Cust_Num__c")
                        {
                            if (childElement.InnerText != "")
                            {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }

        public bool shouldProcessInteraction(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (el.LocalName == "Cust_Num__c")
                {
                    if (el.InnerText != "")
                    {
                        return true;
                    }
                }             
            }
            return false;
        }

        public bool shouldProcessConversation(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (el.LocalName == "Syteline_Interaction_Id__c")
                {
                    if (el.InnerText != "")
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public bool isContactUpdate(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (el.LocalName == "Contact__c")
                {
                    if (el.InnerText != "")
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public bool isInteractionUpdate(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (el.LocalName == "External_Id__c")
                {
                    if (el.InnerText != "")
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public bool isConversationUpdate(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (el.LocalName == "External_Id__c")
                {
                    if (el.InnerText != "")
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public bool shouldProcessProspectContact(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (!hasSytelineProspectId(xmlElements)) { return false; }
                if (el.LocalName == "Contact__c")
                {
                    if (el.InnerText != "" && !hasProspectContactXRef(xmlElements))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        
        public bool shouldProcessCustomerContact(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (!hasSytelineCustNum(xmlElements)) { return false; }
                if (el.LocalName == "Contact__c")
                {
                    if (el.InnerText != "" && !hasCustomerContactXRef(xmlElements))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        private bool hasProspectContactXRef(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (el.LocalName == "Prospect_Contact_Row_Pointer__c")
                {
                    if (el.InnerText != "")
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        private bool hasCustomerContactXRef(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (el.LocalName == "Customer_Contact_Row_Pointer__c")
                {
                    if (el.InnerText != "")
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public bool hasSytelineProspectId(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (el.LocalName == "Account")
                {
                    foreach (XmlElement childElement in el.ChildNodes)
                    {
                        if (childElement.LocalName == "Prospect__c")
                        {
                            if (el.InnerText != "")
                            {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }

        public bool hasSytelineCustNum(XmlElement[] xmlElements)
        {
            foreach (XmlElement el in xmlElements)
            {
                if (el.LocalName == "Account")
                {
                    foreach (XmlElement childElement in el.ChildNodes)
                    {
                        if (childElement.LocalName == "Cust_Num__c")
                        {
                            if (el.InnerText != "")
                            {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }


        public string createUpdateStmt(sObject sObject, List<FieldMapping> fieldMappings, string sourceTable, string destTable, string contextName)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(createInitSubSession(contextName));
            sb.AppendFormat(" exec sp_executesql N'UPDATE {0} SET ", destTable);
            sb.Append(createSetStmt(fieldMappings, sObject));
            sb.AppendFormat(" FROM {0} AS {1}", sourceTable, destTable);
            sb.AppendFormat(" WHERE {0}.[RowPointer] = @RowPointer", destTable);
            sb.AppendFormat(" SELECT @@ROWCOUNT',N'{0}'", buildParameterList(fieldMappings, sObject));
            sb.AppendFormat(",{0}", buildArgumentList(fieldMappings, sObject, true));
            return sb.ToString();
        }

        public string createCustContactXrefUpdateStmt(string destTable, string sfID, string custNum, string contactID)
        {
            StringBuilder sb = new StringBuilder();      
            sb.AppendFormat(" exec sp_executesql N'UPDATE {0} SET Uf_SFUID=''{1}''", destTable, sfID);                  
            sb.AppendFormat(" WHERE cust_num = ''{0}'' And contact_id = {1}'", custNum, contactID);          
            return sb.ToString();
        }

        public string createProspectContactXrefUpdateStmt(string destTable, string sfID, string prospectID, string contactID)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat(" exec sp_executesql N'UPDATE {0} SET Uf_SFUID=''{1}''", destTable, sfID);
            sb.AppendFormat(" WHERE prospect_id = {0} And contact_id = {1}'", prospectID, contactID);
            return sb.ToString();
        }

        public string createInsertStmt(sObject sObject, List<FieldMapping> fieldMappings, string destTableName, string contextName)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(createInitSubSession(contextName));
            sb.AppendFormat("exec sp_executesql N'INSERT {0}", destTableName);
            sb.AppendFormat("({0})", buildColumnsList(fieldMappings, sObject));
            sb.AppendFormat("VALUES ({0})", buildValuesList(fieldMappings, sObject));
            sb.AppendFormat("SELECT @@ROWCOUNT',N'{0}'", buildParameterList(fieldMappings, sObject));
            sb.AppendFormat(",{0}", buildArgumentList(fieldMappings, sObject, false));
            return sb.ToString();
        }
        public string createSetStmt(List<FieldMapping> fieldMappings, sObject so)
        {
            StringBuilder sb = new StringBuilder();
            foreach (XmlElement el in so.Any)
            {
                foreach (FieldMapping fm in fieldMappings)
                {
                    if (el.LocalName.ToLower() == fm.FromField.Replace("[", "").Replace("]", "").ToLower())
                    {
                        //don't update id columns or unique identifiers
                        if (fm.IsUnique || fm.DestFieldDataType == "uniqueidentifier")
                        { continue; }
                        //omit blank or null fields
                        if (String.IsNullOrEmpty(el.InnerText)) { continue; }
                        sb.AppendFormat("{0} = @{1},", fm.ToField, fm.ToField.Replace("[", "").Replace("]", ""));
                    }
                }
            }
            sb.Remove(sb.Length - 1, 1);
            return sb.ToString();
        }

        public string buildColumnsList(List<FieldMapping> fieldMappings, sObject so)
        {
            StringBuilder sb = new StringBuilder();
            foreach (XmlElement el in so.Any)
            {
                foreach (FieldMapping fm in fieldMappings)
                {
                    if (el.LocalName.ToLower() == fm.FromField.Replace("[", "").Replace("]", "").ToLower())
                    {
                        if (!String.IsNullOrEmpty(el.InnerText))
                        {
                            sb.AppendFormat("{0},", fm.ToField);
                        }
                    }
                }
            }
            sb.Remove(sb.Length - 1, 1);
            return sb.ToString();
        }

        public string buildValuesList(List<FieldMapping> fieldMappings, sObject so)
        {
            StringBuilder sb = new StringBuilder();
            foreach (XmlElement el in so.Any)
            {
                foreach (FieldMapping fm in fieldMappings)
                {
                    if (el.LocalName.ToLower() == fm.FromField.Replace("[", "").Replace("]", "").ToLower())
                    {
                        if (!String.IsNullOrEmpty(el.InnerText))
                        {
                            sb.AppendFormat("@{0},", fm.ToField.Replace("[", "").Replace("]", ""));
                        }
                    }
                }
            }
            sb.Remove(sb.Length - 1, 1);
            return sb.ToString();
        }

        public string buildParameterList(List<FieldMapping> fieldMappings, sObject so)
        {
            StringBuilder sb = new StringBuilder();
            foreach (XmlElement el in so.Any)
            {
                foreach (FieldMapping fm in fieldMappings)
                {
                    if (el.LocalName.ToLower() == fm.FromField.Replace("[", "").Replace("]", "").ToLower())
                    {
                        if (String.IsNullOrEmpty(el.InnerText)) { continue; }
                        if (fm.DestFieldDataType.ToLower() == "nvarchar" || fm.DestFieldDataType.ToLower() == "varchar")
                        {
                            sb.AppendFormat("@{0} {1}({2}),", fm.ToField.Replace("[", "").Replace("]", ""), fm.DestFieldDataType, Convert.ToString(fm.DestFieldLength) == "-1" ? "MAX" : fm.DestFieldLength.ToString());
                        }
                        else
                        {
                            sb.AppendFormat("@{0} {1},", fm.ToField.Replace("[", "").Replace("]", ""), fm.DestFieldDataType);
                        }
                    }
                }
            }
            sb.Remove(sb.Length - 1, 1);         
            return sb.ToString();
        }

        public string buildArgumentList(List<FieldMapping> fieldMappings, sObject sObject, bool isUpdate)
        {
            StringBuilder sb = new StringBuilder();
            foreach (XmlElement el in sObject.Any)
            {
                foreach (FieldMapping fm in fieldMappings)
                {
                    //check to see if the Salesforce field is mapped
                    if (el.LocalName.ToLower() == fm.FromField.Replace("[", "").Replace("]", "").ToLower())
                    {
                        if (String.IsNullOrEmpty(el.InnerText)) { continue; }
                        //surround strings with single quotes
                        if (fm.DestFieldDataType.ToLower() == "nvarchar" || fm.DestFieldDataType.ToLower() == "varchar"
                            || fm.DestFieldDataType.ToLower() == "datetime" || fm.DestFieldDataType.ToLower() == "uniqueidentifier"
                            || fm.DestFieldDataType.ToLower() == "nchar")
                        {
                            //generate a new guid if we are inserting
                            if (fm.DestFieldDataType.ToLower() == "uniqueidentifier" && !isUpdate)
                            {
                                sb.AppendFormat("@{0}=N'{1}',", fm.ToField.Replace("[", "").Replace("]", ""), Guid.NewGuid());
                            }
                            else
                            {
                                sb.AppendFormat("@{0}=N'{1}',", fm.ToField.Replace("[", "").Replace("]", ""), el.InnerText.Replace("'","''"));
                            }
                        }
                        else//handling fields that don't require single quotes
                        {
                            //convert true/false to tinyint
                            if (fm.DestFieldDataType.ToLower() == "tinyint")
                            {
                                if (el.InnerText.ToLower() == "false")
                                {
                                    sb.AppendFormat("@{0}={1},", fm.ToField.Replace("[", "").Replace("]", ""), 0);
                                }
                                else if (el.InnerText.ToLower() == "true")
                                {
                                    sb.AppendFormat("@{0}={1},", fm.ToField.Replace("[", "").Replace("]", ""), 1);
                                }
                                else { sb.AppendFormat("@{0}={1},", fm.ToField.Replace("[", "").Replace("]", ""), el.InnerText); }

                            }
                            else { sb.AppendFormat("@{0}={1},", fm.ToField.Replace("[", "").Replace("]", ""), el.InnerText); }
                        }
                    }
                }
            }
            sb.Remove(sb.Length - 1, 1);
            return sb.ToString();
        }


        public string createInitSubSession(string contextName)
        {
            StringBuilder sb = new StringBuilder();
            Guid guid = Guid.NewGuid();
            sb.AppendFormat("exec InitSession2SubSp @ID='{0}',@UserName=NULL,@ConfigName=NULL,@FrameworkSession=0,@Site=N'TR',@ContextName=N'{1}' ", guid, contextName);
            return sb.ToString();
        }

        public void unmapSytelineContactId()
        {
            List<FieldMapping> fieldMappings = new List<FieldMapping>();
            foreach (FieldMapping fm in Process.FieldMappings)
            {
                if (fm.FromField == "Contact__c")
                {
                    Process.FieldMappings.Remove(fm);
                    return;
                }
            }
        }


        public override void doNullFieldCheck(FieldMapping mapping)
        {
            //List<string> fieldsToNull = new List<String>();
            if (mapping.FromField == "sales_team_id")
            {
                so.fieldsToNull[so.fieldsToNull.Length - 1] = mapping.ToField;
            }          
        }

        public override void PopulateISTable()
        {
            // SqlParameter returnParameter = new SqlParameter();
            DataSet ds = new DataSet();
            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            using (SqlConnection connection = new SqlConnection(stagingDbConnString))
            {
                stopwatch.Start();
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = Process.RetrievalSprocName;
                    command.Parameters.AddWithValue("@JobId", JobId);
                    command.Parameters.AddWithValue("@LastRunDate", LastRunDate);
                    command.CommandTimeout = 120;                  
                    new SqlDataAdapter(command).Fill(ds);
                }
                connection.Close();
                stopwatch.Stop();
            }
            //if an error occured
            StringBuilder sb = new StringBuilder();           
            sb.AppendFormat("There were {0} rows inserted into table {1}", Convert.ToInt32(ds.Tables[0].Rows[0][0]), Process.ToObjectName);
            CreateJobLogSyncProcessEntry(Process.ID, sb.ToString(), 2, stopwatch.Elapsed);
        }

        public DataSet GetSFToSytelineSyncErrors()
        {
            DataSet ds = new DataSet();
            using (SqlConnection connection = new SqlConnection(stagingDbConnString))
            {
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = Process.ErrorReportSprocName;
                    command.Parameters.AddWithValue("@JobID", JobId);
                    command.Parameters.AddWithValue("@IsError", 1);
                    command.Parameters.AddWithValue("@SyncProcessID", Process.ID);
                    command.CommandTimeout = 600;
                    new SqlDataAdapter(command).Fill(ds);
                }
                connection.Close();
            }
            return ds;
        }

        public string BuildSoqlQueryFilter()
        {          
            DateTime targetDate = LastRunDate.ToUniversalTime();
            StringBuilder sbFilter = new StringBuilder();
            if (Process.ToObjectName == "Prospect")
            {               
                sbFilter.AppendFormat("Where LastModifiedDate >= {0:yyyy-MM-ddThh:mm:ss.sssZ} And Account.RecordType.Name = 'Prospect' AND IsDeleted=false", targetDate);              				
            }
            else if (Process.ToObjectName == "Syteline Prospect Contact X Ref")
            {
                sbFilter.AppendFormat("Where LastModifiedDate >= {0:yyyy-MM-ddThh:mm:ss.sssZ} AND IsDeleted=false", targetDate);
                sbFilter.Append(" AND Account.Prospect__c != NULL");
                sbFilter.Append(" AND Contact.Contact__c != NULL");
                sbFilter.Append(" AND Syteline_RowPointer__c = NULL");
            }
            else if (Process.ToObjectName == "Syteline Customer Contact X Ref" || Process.ToObjectName == "Syteline Salesperson Contact X Ref")
            {
                sbFilter.AppendFormat("Where LastModifiedDate >= {0:yyyy-MM-ddThh:mm:ss.sssZ} AND IsDeleted=false", targetDate);               
                sbFilter.Append(" AND Account.Cust_Num__c != NULL");
                sbFilter.Append(" AND Contact.Contact__c != NULL");
                sbFilter.Append(" AND Syteline_RowPointer__c = NULL");
            }
        
            else
            {
                sbFilter.AppendFormat("Where LastModifiedDate >= {0:yyyy-MM-ddThh:mm:ss.sssZ} AND IsDeleted=false", targetDate);
            }
       
            return sbFilter.ToString();
        }

        //ToDo: create a function that queries the AccountContactRelation table

    }
}
