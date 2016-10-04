Imports System.ComponentModel
Imports System.Data
Imports Oracle.DataAccess.Client
Imports System.Data.SqlClient
Imports System.Runtime.InteropServices
Imports System.Web.Mail
Imports System.IO
Imports System.ServiceProcess
Imports MQSSendMails.MQSubscribe
Public Class JobRequest
    Dim strDbPlatform As String
    Dim strConnection As String
    Dim strAppServerName As String
    Dim mDBTransaction As IDbTransaction
    Dim mDBCommand As IDbCommand
    Dim mDBConnection As IDbConnection
    Dim mDBAdapter As IDbDataAdapter
    Dim strSchedulerFilePath As String
    Dim strUserid As String
    Dim intRequestID As Integer
    Dim intScheduleId As Integer
    Dim intGroupId As Integer
    Dim intJobRank As Integer
    Dim iHistoryId As Integer
    Dim myProcess As Process = New Process
    Dim ERROR_FILE_NOT_FOUND As Integer = 2
    Dim ERROR_ACCESS_DENIED As Integer = 5
    Dim dsJobCmd As New DataSet
    Dim dsJobs As New DataSet
    Dim dsSvrName As New DataSet

    Dim strJobOutputType As String
    Dim strMsgTempCode As String
    Dim intIsImplicit As Integer
    Dim intJobOrder As Integer
    Dim strServerIP As String
    Dim dtRundate As Date

    Function RetrieveCommand() As MQSCommonObj.MQSStatus
        'get the parameter values for a given scheduler id
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try
            If Not strDbPlatform Is Nothing Then

                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If

                If objStatus.bStatus = False Then
                    Exit Function
                End If

                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select a.Commandtoexecute,a.jobid,b.name,c.output_file_name,d.name jobtype,c.command,c.cmdexec_success_code,c.name jobname,c.process,c.IsSingleton from sc_scheduled_jobs a,sc_job_schedule_info b,sc_m_job c,sc_m_job_type d where a.scheduleid=b.scheduleid and a.jobid=c.jobid and d.jobtypeid=c.typeid and a.scheduleid = " & intScheduleId
                Else
                    strSql = "select a.Commandtoexecute,a.jobid,b.name,c.output_file_name,d.name jobtype,c.command,c.cmdexec_success_code,c.name jobname,c.process,c.IsSingleton from sc_scheduled_jobs a,sc_job_schedule_info b,sc_m_job c,sc_m_job_type d where a.scheduleid=b.scheduleid and a.jobid=c.jobid and d.jobtypeid=c.typeid and a.scheduleid = " & intScheduleId
                End If
                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBAdapter.SelectCommand = mDBCommand
                mDBAdapter.Fill(dsJobCmd)

                objStatus.bStatus = True
                objStatus.objReturn = dsJobCmd
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "RetrieveCommand:ScheduleId" & intScheduleId & ex.Message & ex.StackTrace, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            RetrieveCommand = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function RetrieveServerName() As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim dsAppSvr As New DataSet
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try
            'gets the application server,notify level,Scheduler details 
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select a.appserver,notify_level,notify_email,a.name schname,b.name grpname,d.name jobname from sc_job_schedule_info a,"
                    strSql = strSql & "sc_m_job_group b,sc_job_group_mapping c,sc_m_job d where a.groupid=b.groupid "
                    strSql = strSql & "and a.groupid=c.groupid and c.jobid=d.jobid and a.scheduleid=" & intScheduleId
                Else
                    strSql = "select a.appserver,notify_level,notify_email,a.name schname,b.name grpname,d.name jobname from sc_job_schedule_info a with (NOLOCK),"
                    strSql = strSql & "sc_m_job_group b with (NOLOCK),sc_job_group_mapping c with (NOLOCK),sc_m_job d with (NOLOCK) where a.groupid=b.groupid "
                    strSql = strSql & "and a.groupid=c.groupid and c.jobid=d.jobid and a.scheduleid=" & intScheduleId
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBAdapter.SelectCommand = mDBCommand
                mDBAdapter.Fill(dsAppSvr)

                objStatus.bStatus = True
                objStatus.objReturn = dsAppSvr
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "RetrieveServerName:SchedulId" & intScheduleId.ToString & ex.Message & "StrSql:" & strSql, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            RetrieveServerName = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function UpdateStatusBeforeExe(ByVal intScheduleId As Integer, ByVal strUserId As String) As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim dtModifyDate As String
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    dtModifyDate = Now.ToShortDateString & " " & Now.ToLongTimeString 'Format(Now, "dd/MM/yyyy HH:mm:ss PM")
                    dtModifyDate = "to_date('" & dtModifyDate & "','dd/mm/yyyy hh:mi:ss PM')"
                    strSql = "update SC_Job_Schedule_Info set status='Running',modified_date=" & dtModifyDate & " where Scheduleid=" & intScheduleId
                Else
                    dtModifyDate = "'" & Format(Now.Today, "yyyy-MM-dd").ToString & " " & Now.ToLongTimeString & "'"
                    strSql = "update SC_Job_Schedule_Info  with (UPDLOCK) set status='Running',modified_date=" & dtModifyDate & " where Scheduleid=" & intScheduleId
                End If
                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserId)
            End If

        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, ex.Message & ex.StackTrace, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            UpdateStatusBeforeExe = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function InsertJobHistory(ByVal iJobId As Integer, ByVal rundate As Date, ByVal runtime As DateTime, ByVal strAppSvr As String, ByVal intRequestId As Integer, ByVal strUserId As String) As MQSCommonObj.MQSStatus
        'inserts the given values into history table and sends the history id
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim dtTmpDate As String
        Dim iHistoryId As Integer
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    dtTmpDate = rundate.ToShortDateString & " " & rundate.ToLongTimeString
                    dtTmpDate = "to_date('" & dtTmpDate & "','dd/mm/yyyy hh:mi:ss PM')"
                    strSql = "insert into SC_Job_History(job_id,schedule_id,message,severity,run_date,run_time,run_duration,Appserver,SCREQUESTID) values ("
                    strSql = strSql & iJobId & "," & intScheduleId & ",'Running',null," & dtTmpDate & ",'" & runtime & "',"
                    strSql = strSql & "null,'" & strAppSvr & "'," & intRequestId & ")"
                Else
                    dtTmpDate = "'" & Format(Now.Today, "yyyy-MM-dd").ToString & " " & Now.ToLongTimeString & "'" 'Format(Now, "MM/dd/yyyy Hh:mm:ss") & "'"
                    strSql = "insert into SC_Job_History(job_id,schedule_id,message,severity,run_date,run_time,run_duration,Appserver,SCREQUESTID) values ("
                    strSql = strSql & iJobId & "," & intScheduleId & ",'Running',null," & dtTmpDate & ",'" & runtime & "',"
                    strSql = strSql & "null,'" & strAppSvr & "'," & intRequestId & ")"
                End If
                mDBCommand.CommandText = strSql
                mDBCommand.ExecuteNonQuery()

                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select SC_JobHistory_SEQ.currval  from dual"
                Else
                    strSql = "select hist_id from sc_job_history with (NOLOCK) where schedule_id=" & intScheduleId
                    strSql = strSql & " and job_id=" & iJobId & " and message='Running' and severity is null and run_date="
                    strSql = strSql & dtTmpDate & " and run_time='" & runtime & "' and run_duration is null"
                    strSql = strSql & " and Appserver='" & strAppSvr & "'"
                End If
                mDBCommand.CommandText = strSql
                iHistoryId = mDBCommand.ExecuteScalar()

                objStatus.bStatus = True
                objStatus.objReturn = iHistoryId
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserId)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "InsertJobHistory:ScheduleId" & strSql & intScheduleId & ex.Message & ex.StackTrace, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            InsertJobHistory = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function sendEmail(ByVal strEmail As String, ByVal strSchName As String, ByVal strGrpname As String, ByVal strJobname As String, ByVal rundate As Date, ByVal lngDuration As Long, ByVal strStatus As String, ByVal runId As Integer, ByVal strProcname As String, ByVal strUserId As String) As MQSCommonObj.MQSStatus
        'send the mails when nitify level is grater than 3
        Dim strSender As String
        Dim strContent As String
        Dim objStatus As New MQSCommonObj.MQSStatus

        Dim strSql As String
        Dim strWebAdminMail As String
        Dim strMessage As String
        Dim strMessage1 As String
        Try
            System.Web.Mail.SmtpMail.SmtpServer = "12" 'objMqsCommon.strServerIP"


            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select email from am_users where upper(user_id)='WEBADMIN'"
                Else
                    strSql = "select email from am_users with (NOLOCK) where upper(user_id)='WEBADMIN'"
                End If

                mDBCommand.CommandText = strSql
                strWebAdminMail = mDBCommand.ExecuteScalar()

                strMessage = "Schedule Name: " & strSchName & vbNewLine
                strMessage = strMessage & "Group Name: " & strGrpname & vbNewLine
                strMessage = strMessage & "Job Name: " & strJobname & vbNewLine
                strMessage = strMessage & "Runtime: " & rundate.Now & vbNewLine
                strMessage = strMessage & "Duration(seconds): " & lngDuration & vbNewLine
                If strStatus = "Completed" Then
                    strMessage1 = "Completed the Job successfully" & vbNewLine
                    strMessage = strMessage1 & strMessage
                    System.Web.Mail.SmtpMail.Send(strWebAdminMail, strEmail, "Executed the '" & strJobname & "' Job successfully", strMessage)
                ElseIf strStatus = "Failed" Then
                    strMessage1 = "Failed to execute the job" & vbNewLine
                    strMessage = strMessage1 & strMessage
                    System.Web.Mail.SmtpMail.Send(strWebAdminMail, strEmail, "Failed to execute '" & strJobname & "' job", strMessage)
                ElseIf strStatus = "Stopped" Then
                    strMessage1 = "Execution of the job is stopped" & vbNewLine
                    strMessage = strMessage & "RunProcess: " & strProcname & vbNewLine
                    strMessage = strMessage & "RunId: " & runId & vbNewLine
                    strMessage = strMessage1 & strMessage
                    System.Web.Mail.SmtpMail.Send(strWebAdminMail, strEmail, "Stopped execution '" & strJobname & "' job", strMessage)
                End If
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserId)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "SendEMail:" & ex.Message & ex.StackTrace, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            sendEmail = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function chkStatusOfBatch(ByRef blnStop As Boolean, ByVal iHistoryId As Integer, ByVal strProcName As String)
        'on process exit check the status of the batch is stoped or not
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim i As Int16
        Dim tmpOutParam As IDbDataParameter
        Dim tmpOutParamStatus As IDbDataParameter
        Dim objLANG_ID As IDbDataParameter
        Dim objUSERINTERFACE_ID As IDbDataParameter
        Dim objMESSAGE As IDbDataParameter
        Dim objIS_VALIDATION_SUCCESS As IDbDataParameter
        Dim tempParam As IDbDataParameter
        Dim tempProcName As IDbDataParameter
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                mDBCommand.CommandText = "MQSP_CHECKSTATUSOFBATCHJOBS"
                mDBCommand.CommandType = CommandType.StoredProcedure

                If strDbPlatform.ToUpper = "ORACLE" Then
                    tempParam = New OracleParameter
                    tempParam.ParameterName = "RUN_ID"
                    tempParam.Value = iHistoryId
                    tempParam.Direction = ParameterDirection.Input
                    tempParam.DbType = DbType.Int32
                    tempProcName = New OracleParameter
                    tempProcName.ParameterName = "PROCESS_NAME"
                    tempProcName.Value = strProcName
                    tempProcName.Direction = ParameterDirection.Input
                    tempProcName.DbType = DbType.String
                    tmpOutParam = New OracleParameter
                    tmpOutParam.ParameterName = "BLN_STOPPED"
                    tmpOutParam.Direction = ParameterDirection.Output
                    tmpOutParam.DbType = DbType.String
                    tmpOutParam.Size = 10
                Else
                    tempParam = New SqlParameter
                    tempParam.ParameterName = "@" + "RUN_ID"
                    tempParam.Value = iHistoryId
                    tempParam.Direction = ParameterDirection.Input
                    tempParam.DbType = DbType.Int32
                    tempParam.Size = 9
                    tempProcName = New SqlParameter
                    tempProcName.ParameterName = "@" + "PROCESS_NAME"
                    tempProcName.Value = strProcName
                    tempProcName.Direction = ParameterDirection.Input
                    tempProcName.DbType = DbType.String
                    tempProcName.Size = 100
                    tmpOutParam = New SqlParameter
                    tmpOutParam.ParameterName = "@" + "BLN_STOPPED"
                    tmpOutParam.Direction = ParameterDirection.Output
                    tmpOutParam.DbType = DbType.String
                    tmpOutParam.Size = 10
                End If
                mDBCommand.Parameters.Add(tempParam)
                mDBCommand.Parameters.Add(tempProcName)
                mDBCommand.Parameters.Add(tmpOutParam)
                mDBCommand.ExecuteNonQuery()

                If strDbPlatform.ToUpper = "ORACLE" Then
                    tmpOutParam = mDBCommand.Parameters.Item("BLN_STOPPED")
                Else
                    tmpOutParam = mDBCommand.Parameters.Item("@BLN_STOPPED")
                End If
                blnStop = tmpOutParam.Value
                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "chkStatusOfBatch:HistoryID" & iHistoryId & ex.Message & ex.StackTrace, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            chkStatusOfBatch = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function RetrieveParamValues(ByVal intScheduleId As Integer, ByVal strUserId As String) As MQSCommonObj.MQSStatus
        'To get the parameter values
        Dim strSql As String
        Dim dsParams As New DataSet
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim objAdapt As Object
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    objAdapt = New OracleDataAdapter
                    strSql = "select a.paramid,value,b.name from SC_Job_parameter_values a ,SC_M_parameter b where a.paramid=b.paramid and scheduleid=" & intScheduleId
                Else
                    strSql = "select a.paramid,value,b.name from SC_Job_parameter_values a with (NOLOCK),SC_M_parameter b with (NOLOCK) where a.paramid=b.paramid and scheduleid=" & intScheduleId
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBAdapter.SelectCommand = mDBCommand
                mDBAdapter.Fill(dsParams)

                objStatus.bStatus = True
                objStatus.objReturn = dsParams
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserId)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "RetrieveParamValues:ScheduleId" & intScheduleId & ex.Message & "strSql" & strSql, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            RetrieveParamValues = objStatus
            objStatus = Nothing
        End Try
    End Function
    Public Sub StartProcess()
        'starts the windows process and sets the new requests
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim intI As Integer
        Dim blnIsSingleton As Boolean = False
        Dim startTime As Date
        Dim strOutFilename As String
        Dim strLogFilename As String
        Dim strExeCmd As String
        Dim strStatus As String
        Dim strCommandFile As String
        Dim intProcessID As Integer
        Dim iExePos As Integer
        Dim strArgs As String
        Dim blnStatus As Boolean
        Dim iHistoryId As Integer
        Dim blnStop As Boolean
        Dim strProcName As String
        Dim dsJobParams As DataSet
        Dim lstRunOutcome As String
        Dim iExitCode As Integer
        Dim strFilePath As String
        Dim strAppSvr As String
        Dim blnStart As Boolean
        Dim strName As String
        Dim dtEndTime As DateTime
        Dim dtstartdatetime As DateTime
        Dim i As Int16
        Dim j As Int16

        Dim strParameterspos As String
        Dim strProcParams As String
        Dim strProcParamPositions() As String
        Dim ProcArgs As String

        Dim dsParams As New DataSet
        Dim strCommandToExe As String

        Dim intJobid As Integer
        Dim strJobname As String
        Dim strProcess As String
        Dim strFileName_SP As String
        Dim strParamvalue As String
        Dim strProcOutput As String
        Try

            Dim ci As New System.Globalization.CultureInfo(System.Threading.Thread.CurrentThread.CurrentCulture.LCID)

            ci.DateTimeFormat.ShortDatePattern = "dd/MM/yyyy"
            ci.DateTimeFormat.DateSeparator = "/"
            System.Threading.Thread.CurrentThread.CurrentCulture = ci
            ci.DateTimeFormat.LongTimePattern = "hh:mm:ss tt"

            dtstartdatetime = Now
            strStatus = "Running"

            'Insert Record into History table
            startTime = Now.ToLongTimeString

            If Not dsJobs Is Nothing AndAlso Not dsJobs.Tables(0) Is Nothing Then
                If dsJobs.Tables(0).Rows.Count > 0 Then

                    objStatus = InsertJobHistory(dsJobs.Tables(0).Rows(i).Item("jobid"), Now, startTime, strAppServerName, intRequestID, strUserid)
                    If objStatus.bStatus = False Then
                        abortTransaction()
                        MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "StartProcess : Unable to Add History:" & intScheduleId.ToString & objStatus.strErrDescription, strUserid)
                        UpdateStatusAsStop()
                        Exit Sub
                    Else
                        'get the Histroy Id
                        iHistoryId = objStatus.objReturn
                        intJobid = CType(dsJobs.Tables(0).Rows(0).Item("jobid"), Integer)
                        strJobname = dsJobs.Tables(0).Rows(0).Item(3).ToString
                        strProcess = dsJobs.Tables(0).Rows(0).Item("process").ToString
                        strProcName = dsJobs.Tables(0).Rows(0).Item("PREPROCESS_SP").ToString
                        strParameterspos = dsJobs.Tables(0).Rows(0).Item("PREPROCESS_SP_PARAM_POS").ToString
                        strFileName_SP = dsJobs.Tables(0).Rows(0).Item("OUTPUT_FILENAME_SP").ToString


                        If dsJobs.Tables(0).Rows(0).Item(10).ToString.ToUpper = "REPORTS" Then
                            'CALL THE REPORT 
                            objStatus = GetReportArgs(intJobid, strJobname, strProcName, strParameterspos, strProcess, strFileName_SP, iHistoryId, strOutFilename, strLogFilename)
                            If objStatus.bStatus = False Then
                                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "ScheduleID: " & intScheduleId.ToString & " and Job Id : " & intJobid & objStatus.strErrDescription, strUserid)
                                abortTransaction()
                                UpdateStatusAsStop()
                                Exit Sub
                            End If
                            strArgs = objStatus.objReturn
                        Else
                            ' frame the output file name and logfile name in the following format
                            ' Fomat for output file is:- RunId_outputfilename
                            ' Fomat for log file is:- RunId.log

                            If Not strFileName_SP = Nothing Or Not strFileName_SP = String.Empty Then
                                objStatus = GetJoboutputFileName(intJobid, strFileName_SP)
                                If objStatus.bStatus = False Then
                                    objStatus.bStatus = False
                                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : StartProcess " & objStatus.strErrDescription, strUserid)
                                    ' Return objStatus
                                    strOutFilename = iHistoryId & "_" & dsJobs.Tables(0).Rows(i).Item("OUTPUT_FILE_NAME")
                                    If Not dsJobs.Tables(0).Rows(0).Item("process") Is System.DBNull.Value Then
                                        If dsJobs.Tables(0).Rows(0).Item("process") <> "" Then
                                            strLogFilename = iHistoryId & "_" & dsJobs.Tables(0).Rows(i).Item("process") & ".log"
                                        End If
                                    Else
                                        strLogFilename = iHistoryId & ".log"
                                    End If
                                Else
                                    strOutFilename = objStatus.objReturn & ".txt"
                                    strLogFilename = objStatus.objReturn & ".log"
                                End If
                            Else
                                strOutFilename = iHistoryId & "_" & dsJobs.Tables(0).Rows(i).Item("OUTPUT_FILE_NAME")
                                If Not dsJobs.Tables(0).Rows(0).Item("process") Is System.DBNull.Value Then
                                    If dsJobs.Tables(0).Rows(0).Item("process") <> "" Then
                                        strLogFilename = iHistoryId & "_" & dsJobs.Tables(0).Rows(i).Item("process") & ".log"
                                    End If
                                Else
                                    strLogFilename = iHistoryId & ".log"
                                End If
                            End If

                            'construct the parameters
                            objStatus = GetJobParameterValues(CType(dsJobs.Tables(0).Rows(i).Item("jobid"), Integer))
                            If objStatus.bStatus = False Then
                                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "ScheduleID: " & intScheduleId.ToString & " and Job Id : " & intJobid & objStatus.strErrDescription, strUserid)
                                abortTransaction()
                                UpdateStatusAsStop()
                                Exit Sub
                            End If
                            dsParams = objStatus.objReturn

                            If Not dsParams Is Nothing AndAlso Not dsParams.Tables(0) Is Nothing Then
                                If dsParams.Tables(0).Rows.Count > 0 Then
                                    For j = 0 To dsParams.Tables(0).Rows.Count - 1
                                        If Not dsParams.Tables(0).Rows(j).Item("PARAM_VAL_TYPE_ID") Is System.DBNull.Value Then
                                            If CInt(dsParams.Tables(0).Rows(j).Item("PARAM_VAL_TYPE_ID")) = 2 Then
                                                objStatus = GetProcOutput(dsParams.Tables(0).Rows(j).Item("value").ToString, strUserid)
                                                If objStatus.bStatus = False Then
                                                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "ScheduleID: " & intScheduleId.ToString & " and Job Id : " & intJobid & objStatus.strErrDescription, strUserid)
                                                    abortTransaction()
                                                    UpdateStatusAsStop()
                                                    Exit Sub
                                                End If
                                                strParamvalue = objStatus.objReturn
                                                objStatus = insertDynamicParamValue(intRequestID, intScheduleId, intJobid, CInt(dsParams.Tables(0).Rows(j).Item("PARAMID")), strParamvalue)
                                                If objStatus.bStatus = False Then
                                                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "ScheduleID: " & intScheduleId.ToString & " and Job Id : " & intJobid & objStatus.strErrDescription, strUserid)
                                                    abortTransaction()
                                                    UpdateStatusAsStop()
                                                    Exit Sub
                                                End If
                                            ElseIf CInt(dsParams.Tables(0).Rows(j).Item("PARAM_VAL_TYPE_ID")) = 3 Then
                                                objStatus = CustomDateFunction(intScheduleId, intJobid, CInt(dsParams.Tables(0).Rows(j).Item("PARAMID")), strUserid)
                                                If objStatus.bStatus = False Then
                                                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "ScheduleID: " & intScheduleId.ToString & " and Job Id : " & intJobid & objStatus.strErrDescription, strUserid)
                                                    abortTransaction()
                                                    UpdateStatusAsStop()
                                                    Exit Sub
                                                End If
                                                strParamvalue = objStatus.objReturn
                                                objStatus = insertDynamicParamValue(intRequestID, intScheduleId, intJobid, CInt(dsParams.Tables(0).Rows(j).Item("PARAMID")), strParamvalue)
                                                If objStatus.bStatus = False Then
                                                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "ScheduleID: " & intScheduleId.ToString & " and Job Id : " & intJobid & objStatus.strErrDescription, strUserid)
                                                    abortTransaction()
                                                    UpdateStatusAsStop()
                                                    Exit Sub
                                                End If
                                            Else
                                                strParamvalue = dsParams.Tables(0).Rows(j).Item("value").ToString
                                            End If
                                        Else
                                            strParamvalue = dsParams.Tables(0).Rows(j).Item("value").ToString
                                        End If
                                        strCommandToExe = strCommandToExe & dsParams.Tables(0).Rows(j).Item("name").ToString & "=" & strParamvalue & "|"
                                    Next
                                End If
                            End If

                            Dim strOutputDir As String
                            objStatus = Me.getOutputFolder(intJobid, strUserid)
                            strOutputDir = CStr(objStatus.objReturn)


                            If strCommandToExe = Nothing OrElse strCommandToExe.Trim = String.Empty Then
                                strArgs = "Process=" & strProcess & "|" & "OutputFile=" & strOutputDir & "\" & strOutFilename & "|" & "ErrorLogFile=" & strOutputDir & "\" & strLogFilename & "|" & "UserId=" & strUserid & "|" & "RunId=" & iHistoryId
                            Else
                                strArgs = strCommandToExe.Trim & "Process=" & strProcess & "|" & "OutputFile=" & strOutputDir & "\" & strOutFilename & "|" & "ErrorLogFile=" & strOutputDir & "\" & strLogFilename & "|" & "UserId=" & strUserid & "|" & "RunId=" & iHistoryId
                            End If
                        End If
                    End If
                    strExeCmd = dsJobs.Tables(0).Rows(0).Item("command")

                    'set the new request
                    If intJobOrder = 1 Then
                        objStatus = SetNewScheduleRequest()
                        If objStatus.bStatus = False Then
                            abortTransaction()
                            UpdateStatusAsStop()
                            Exit Sub
                        End If
                    End If

                    Try
                        ' Set EnableRaisingEvents to True. 
                        myProcess.EnableRaisingEvents = True
                        ' Add an event handler to trap the Exited event. 
                        'AddHandler myProcess.Exited, AddressOf OnProcessExit

                        myProcess.StartInfo.FileName = strExeCmd
                        myProcess.StartInfo.Arguments = strArgs

                        ' Start the process in a hidden window.
                        myProcess.StartInfo.WindowStyle = ProcessWindowStyle.Hidden
                        myProcess.StartInfo.CreateNoWindow = True

                        'Set the Priority of the Job
                        myProcess.Start()
                        intProcessID = myProcess.Id
                        'Update the process id into History Table.
                        objStatus = UpdateJobHistoryProcessId(iHistoryId, intProcessID)
                        If objStatus.bStatus = False Then
                            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "ScheduleID: " & intScheduleId.ToString & " and Job Id : " & intJobid & objStatus.strErrDescription, strUserid)
                            abortTransaction()
                            UpdateStatusAsStop()
                            Exit Sub
                        End If
                    Catch e As Win32Exception
                        If e.NativeErrorCode = ERROR_FILE_NOT_FOUND Then
                            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, e.Message & "Scheduler Id is :" & intScheduleId.ToString & "Check the File Path :" & strExeCmd, strUserid)
                            abortTransaction()
                            UpdateStatusAsStop()
                            Exit Sub
                        Else
                            If e.NativeErrorCode = ERROR_ACCESS_DENIED Then
                                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, e.Message & "Scheduler Id is :" & intScheduleId.ToString & "You do not have permission to Execute this file :" & strExeCmd, strUserid)
                                abortTransaction()
                                UpdateStatusAsStop()
                                Exit Sub
                            End If
                        End If
                    Catch ex As Exception
                        MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "ScheduleID:" & intScheduleId.ToString & ex.Message & Ex.StackTrace & "File :" & strExeCmd, strUserid)
                        abortTransaction()
                        UpdateStatusAsStop()
                        Exit Sub
                    End Try
                    CommitTransaction()
                    myProcess.WaitForExit()

                    If strExeCmd.Trim.ToUpper = "CCMD.EXE" Then
                        If myProcess.ExitCode = 0 Then
                            UpdateReportStatus("S", iHistoryId)
                        Else
                            UpdateReportStatus("F", iHistoryId)
                        End If
                    End If
                    myProcess.Dispose()
                    dtEndTime = Now
                    If dsJobs.Tables(0).Rows(i).Item(10).ToString.ToUpper = "BATCHJOBS" Then
                        strProcName = dsJobs.Tables(0).Rows(i).Item("process")
                        objStatus = chkStatusOfBatch(blnStop, iHistoryId, strProcName)
                        mDBCommand.Parameters.Clear()
                        If blnStop = True Then
                            strStatus = "Stopped"
                            lstRunOutcome = "Stopped"
                            ' if notify level is 1 or 3 i.e send email on success or always case then 
                            ' send email after executing shell command 
                            If Not dsSvrName.Tables(0).Rows(0).Item("notify_level") Is System.DBNull.Value Then
                                If Convert.ToInt32(dsSvrName.Tables(0).Rows(0).Item("notify_level")) = 3 Then 'i.e notify level is 'Always'
                                    ' send Email
                                    objStatus = sendEmail(dsSvrName.Tables(0).Rows(0).Item("notify_email"), dsSvrName.Tables(0).Rows(0).Item("schname"), dsSvrName.Tables(0).Rows(0).Item("grpname"), dsSvrName.Tables(0).Rows(0).Item("jobname"), Now.Today, DateDiff(DateInterval.Second, startTime, dtEndTime), strStatus, iHistoryId, strProcName, strUserid)
                                    If objStatus.bStatus = False Then
                                        MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, objStatus.strErrDescription, strUserid)
                                    End If
                                End If
                            End If
                        Else
                            strStatus = "Completed"
                            If Not dsSvrName.Tables(0).Rows(0).Item("notify_level") Is System.DBNull.Value Then
                                If Convert.ToInt32(dsSvrName.Tables(0).Rows(0).Item("notify_level")) = 1 Or Convert.ToInt32(dsSvrName.Tables(0).Rows(0).Item("notify_level")) = 3 Then
                                    ' send Email
                                    objStatus = sendEmail(dsSvrName.Tables(0).Rows(0).Item("notify_email"), dsSvrName.Tables(0).Rows(0).Item("schname"), dsSvrName.Tables(0).Rows(0).Item("grpname"), dsSvrName.Tables(0).Rows(0).Item("jobname"), Now.Today, DateDiff(DateInterval.Second, startTime, dtEndTime), strStatus, iHistoryId, strProcName, strUserid)
                                    If objStatus.bStatus = False Then
                                        MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, objStatus.strErrDescription, strUserid)
                                    End If
                                End If
                            End If
                        End If
                    Else
                        strStatus = "Completed"
                        If Not dsSvrName.Tables(0).Rows(0).Item("notify_level") Is System.DBNull.Value Then
                            If Convert.ToInt32(dsSvrName.Tables(0).Rows(0).Item("notify_level")) = 1 Or Convert.ToInt32(dsSvrName.Tables(0).Rows(0).Item("notify_level")) = 3 Then
                                ' send Email
                                objStatus = sendEmail(dsSvrName.Tables(0).Rows(0).Item("notify_email"), dsSvrName.Tables(0).Rows(0).Item("schname"), dsSvrName.Tables(0).Rows(0).Item("grpname"), dsSvrName.Tables(0).Rows(0).Item("jobname"), Now.Today, DateDiff(DateInterval.Second, startTime, dtEndTime), strStatus, iHistoryId, strProcName, strUserid)
                                If objStatus.bStatus = False Then
                                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, objStatus.strErrDescription, strUserid)
                                End If
                            End If
                        End If
                    End If
                    ' update history as stopped or success based on sp output in case of batch jobs                                                                        
                    objStatus = UpdateJobHistory(iHistoryId, strStatus, strOutFilename, strLogFilename, DateDiff(DateInterval.Second, dtstartdatetime, dtEndTime), strUserid, intRequestID)
                    If objStatus.bStatus = False Then
                        MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, objStatus.strErrDescription, strUserid)
                    End If
                    If intIsImplicit = 1 Then
                        objStatus = UpdateScheduleRequest("CO", iHistoryId)
                    Else
                        objStatus = UpdateExpGroupRequest()
                    End If

                    If objStatus.bStatus = False Then
                        MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, objStatus.strErrDescription, strUserid)
                    End If
                    objStatus = UpdateScheduleInfoStatus(strStatus, DateDiff(DateInterval.Second, dtstartdatetime, dtEndTime), dtstartdatetime)
                    If objStatus.bStatus = False Then
                        MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, objStatus.strErrDescription, strUserid)
                    End If

                    'sending the job outputs by mail
                    If intIsImplicit = 1 Then
                        'Sending Job Outputs through Email in case of Jobs 
                        objStatus = GetJobOutcome(iHistoryId)
                        If objStatus.bStatus = False Then
                            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, objStatus.strErrDescription, strUserid)
                        Else
                            If objStatus.objReturn.ToString.ToUpper.Equals("TRUE") Then
                                objStatus = SendJobOutputByEmail(intScheduleId, intRequestID, iHistoryId, intGroupId)
                                If objStatus.bStatus Then
                                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQServices : JobRequest :" + objStatus.strErrDescription, strUserid)
                                End If
                            End If
                        End If
                    Else
                        'Sending Job Outputs through Email in case of Job Groups
                        objStatus = IsCompleted(intRequestID)
                        If objStatus.bStatus = False Then
                            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, objStatus.strErrDescription, strUserid)
                        Else
                            If objStatus.objReturn.ToString.ToUpper.Equals("TRUE") Then
                                objStatus = GetJobOutcome(iHistoryId)
                                If objStatus.bStatus = False Then
                                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, objStatus.strErrDescription, strUserid)
                                Else
                                    If objStatus.objReturn.ToString.ToUpper.Equals("TRUE") Then
                                        SendJobOutputByEmail(intScheduleId, intRequestID, iHistoryId, intGroupId)
                                    End If
                                End If
                            End If
                        End If
                    End If
                    CommitTransaction()
                    Exit Sub
                    objStatus.bStatus = True
                End If
            End If
        Catch ex As Exception
            If iHistoryId = 0 Then
                abortTransaction()
            Else
                CommitTransaction()
            End If
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "StartProcess:Exception For RequstId:" & intRequestID.ToString & ex.Message & "FileName:" & strExeCmd, strUserid)
            UpdateStatusAsStop()
        Finally
            If Not mDBConnection Is Nothing Then
                If mDBConnection.State = ConnectionState.Open Then
                    mDBConnection.Close()
                End If
            End If
        End Try
    End Sub
    Function CustomDateFunction(ByVal intScheduleId As Integer, ByVal intJobId As Integer, ByVal intParamId As Integer, ByVal strUser As String) As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim objstatus As New MQSCommonObj.MQSStatus
        Dim dscustdateinput As New DataSet
        Dim strMonthVal As String
        Dim strDayVal As String
        Dim dtResultDate As Date

        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objstatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objstatus = openConnection()
                End If
            Else
                objstatus.bStatus = False
                Return objstatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : CustomDateFunction: ScheduleID:" & intScheduleId & " JobId : " & intJobId & "Parameter Id :" & intParamId & objstatus.strErrDescription, strUserid)
            End If
            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "Select MONTH_VALUE, DAY_VALUE, MON_OPR,NO_OF_MONTHS, NO_OF_DAYS,DAY_OPR from SC_DATEFUN_PARAM where SCHEDULEID=" & intScheduleId & " and jobid=" & intJobId & " and paramid=" & intParamId
            Else
                strSql = "Select MONTH_VALUE, DAY_VALUE, MON_OPR,NO_OF_MONTHS, NO_OF_DAYS,DAY_OPR from SC_DATEFUN_PARAM With (NOLOCK) where SCHEDULEID=" & intScheduleId & " and jobid=" & intJobId & " and paramid=" & intParamId
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            mDBAdapter.SelectCommand = mDBCommand
            mDBAdapter.Fill(dscustdateinput)

            If dscustdateinput Is Nothing OrElse dscustdateinput.Tables(0) Is Nothing OrElse dscustdateinput.Tables(0).Rows.Count = 0 Then
                objstatus.bStatus = False
                objstatus.strErrDescription = " No Date Present For Custom Date Calculation."
                Return objstatus
            End If

            If CInt(dscustdateinput.Tables(0).Rows(0).Item("MONTH_VALUE")) = 0 Then
                strMonthVal = Month(DateTime.Today).ToString
            Else
                strMonthVal = dscustdateinput.Tables(0).Rows(0).Item("MONTH_VALUE").ToString
            End If

            If CInt(dscustdateinput.Tables(0).Rows(0).Item("DAY_VALUE")) = 0 Then
                strDayVal = Day(DateTime.Today).ToString
            Else
                strDayVal = dscustdateinput.Tables(0).Rows(0).Item("DAY_VALUE").ToString
            End If

            dtResultDate = Format(Convert.ToDateTime(strDayVal + "/" + strMonthVal + "/" + Year(DateTime.Today).ToString), "dd/MM/yyyy")

            If CStr(dscustdateinput.Tables(0).Rows(0).Item("MON_OPR")).ToUpper.Equals("ADD") Then
                dtResultDate = dtResultDate.AddMonths(CInt(dscustdateinput.Tables(0).Rows(0).Item("NO_OF_MONTHS")))
            ElseIf CStr(dscustdateinput.Tables(0).Rows(0).Item("MON_OPR")).ToUpper.Equals("SUB") Then
                dtResultDate = dtResultDate.AddMonths(-CInt(dscustdateinput.Tables(0).Rows(0).Item("NO_OF_MONTHS")))
            End If

            If CStr(dscustdateinput.Tables(0).Rows(0).Item("DAY_OPR")).ToUpper.Equals("ADD") Then
                dtResultDate = dtResultDate.AddDays(CInt(dscustdateinput.Tables(0).Rows(0).Item("NO_OF_DAYS")))
            ElseIf CStr(dscustdateinput.Tables(0).Rows(0).Item("DAY_OPR")).ToUpper.Equals("SUB") Then
                dtResultDate = dtResultDate.AddDays(-CInt(dscustdateinput.Tables(0).Rows(0).Item("NO_OF_DAYS")))
            End If


            objstatus.bStatus = True
            objstatus.objReturn = dtResultDate
            Return objstatus
        Catch ex As Exception
            objstatus.bStatus = False
            objstatus.strErrDescription = ex.Message
            Return objstatus
        Finally
            objstatus = Nothing
        End Try
    End Function
    Function insertDynamicParamValue(ByVal intRequestId As Integer, ByVal intScheduleId As Integer, ByVal intJobId As Integer, ByVal intParamId As Integer, ByVal strValue As String) As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim dtTmpDate As String
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "insert into SC_DYNAMIC_PARAM_VALUES(SCHEDULEID,SCREQUESTID,JOBID,PARAMID,VALUE,CREATED_BY, MODIFIED_BY) values ("
                    strSql = strSql & intScheduleId & "," & intRequestId & "," & intJobId & "," & intParamId & ",'" & strValue & "','" & strUserid & "','" & strUserid & "')"
                Else
                    strSql = "insert into SC_DYNAMIC_PARAM_VALUES(SCHEDULEID,SCREQUESTID,JOBID,PARAMID,VALUE,CREATED_BY, MODIFIED_BY) values ("
                    strSql = strSql & intScheduleId & "," & intRequestId & "," & intJobId & "," & intParamId & ",'" & strValue & "','" & strUserid & "','" & strUserid & "')"
                End If
                mDBCommand.CommandType = CommandType.Text
                mDBCommand.Parameters.Clear()
                mDBCommand.CommandText = strSql
                mDBCommand.ExecuteNonQuery()
                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "InsertJobHistory:ScheduleId" & strSql & intScheduleId & ex.Message & ex.StackTrace, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            objStatus = Nothing
        End Try
    End Function

    'Author : Abdul
    'Description : To Execute the stored procedure for dynamic parameter and get the value.
    Function GetProcOutput(ByVal strProcName As String, ByVal strUserid As String) As MQSCommonObj.MQSStatus
        Dim strsql As String
        Dim objProc_Name As Object
        Dim strOutValue As String
        Dim objstatus As New MQSCommonObj.MQSStatus
        Dim strFreqType As String
        Dim dtRunAt As Date


        Dim tmpOutParam As IDbDataParameter
        Dim tmpScReqParam As IDbDataParameter
        Dim tmpLasrRunParam As IDbDataParameter
        Dim tmpRundateParam As IDbDataParameter
        Dim tmpFreqTypeParam As IDbDataParameter

        Dim tmpErrCode As IDbDataParameter
        Dim tmpErrMesg As IDbDataParameter

        Dim dsScheduleinfo As New DataSet

        strOutValue = String.Empty
        'EVERY PROCEDURE SPECIFIED FOR DYNAMIC PARAMETERS HAVE FOLLOWING PARAMETERS
        'P_SCREQID NUMBER,P_LASTRUNDATE DATE,P_RUNAT DATE,P_FREQTYPE NUMBER,RET_VAL OUT VARCHAR
        'intRequestID
        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objstatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objstatus = openConnection()
                End If
                If objstatus.bStatus = False Then
                    Exit Function
                End If

                objstatus = GetScheduleInfo(intRequestID, strUserid)
                If objstatus.bStatus = False Then
                    objstatus.bStatus = False
                    objstatus.strErrDescription = objstatus.strErrDescription
                    Return objstatus
                End If

                dsScheduleinfo = objstatus.objReturn

                strFreqType = dsScheduleinfo.Tables(0).Rows(0).Item("FREQ_TYPE").ToString
                If Not dsScheduleinfo.Tables(0).Rows(0).Item("RUN_DATE") Is System.DBNull.Value Then
                    dtRunAt = CDate(dsScheduleinfo.Tables(0).Rows(0).Item("RUN_DATE"))
                Else
                    objstatus.bStatus = False
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest  : GetProcOutput : Run At Is Null", strUserid)
                    Return objstatus
                End If


                mDBCommand.CommandText = strProcName
                mDBCommand.CommandType = CommandType.StoredProcedure

                If strDbPlatform.ToUpper = "ORACLE" Then

                    tmpScReqParam = New OracleParameter
                    tmpScReqParam.ParameterName = "SC_REQID" 'Request ID
                    tmpScReqParam.Direction = ParameterDirection.Input
                    tmpScReqParam.DbType = DbType.Int32
                    tmpScReqParam.Size = 10
                    tmpScReqParam.Value = intRequestID

                    tmpRundateParam = New OracleParameter
                    tmpRundateParam.ParameterName = "RUN_DATE" 'Run Date
                    tmpRundateParam.Direction = ParameterDirection.Input
                    tmpRundateParam.DbType = DbType.Date
                    tmpRundateParam.Value = dtRundate

                    tmpOutParam = New OracleParameter
                    tmpOutParam.ParameterName = "OUTPUT_VAL"
                    tmpOutParam.Direction = ParameterDirection.Output
                    tmpOutParam.DbType = DbType.String
                    tmpOutParam.Size = 50

                    tmpErrCode = New OracleParameter
                    tmpErrCode.ParameterName = "ERRCODE"
                    tmpErrCode.Direction = ParameterDirection.Output
                    tmpErrCode.DbType = DbType.Int32

                    tmpErrMesg = New OracleParameter
                    tmpErrMesg.ParameterName = "ERRMESG"
                    tmpErrMesg.Direction = ParameterDirection.Output
                    tmpErrMesg.DbType = DbType.String
                    tmpErrMesg.Size = 50

                Else

                    tmpScReqParam = New SqlParameter
                    tmpScReqParam.ParameterName = "SC_REQID"
                    tmpScReqParam.Direction = ParameterDirection.Input
                    tmpScReqParam.DbType = DbType.Int32
                    tmpScReqParam.Size = 10
                    tmpScReqParam.Value = intRequestID

                    tmpRundateParam = New SqlParameter
                    tmpRundateParam.ParameterName = "RUN_DATE"
                    tmpRundateParam.Direction = ParameterDirection.Input
                    tmpRundateParam.DbType = DbType.Date
                    tmpRundateParam.Value = dtRundate

                    tmpOutParam = New SqlParameter
                    tmpOutParam.ParameterName = "OUTPUT_VAL"
                    tmpOutParam.Direction = ParameterDirection.Output
                    tmpOutParam.DbType = DbType.String
                    tmpOutParam.Size = 50

                    tmpErrCode = New SqlParameter
                    tmpErrCode.ParameterName = "ERRCODE"
                    tmpErrCode.Direction = ParameterDirection.Output
                    tmpErrCode.DbType = DbType.Int32

                    tmpErrMesg = New SqlParameter
                    tmpErrMesg.ParameterName = "ERRMESG"
                    tmpErrMesg.Direction = ParameterDirection.Output
                    tmpErrMesg.DbType = DbType.String
                    tmpErrMesg.Size = 50
                End If
            End If
            mDBCommand.Parameters.Add(tmpScReqParam)
            'mDBCommand.Parameters.Add(tmpLasrRunParam)
            mDBCommand.Parameters.Add(tmpRundateParam)
            'mDBCommand.Parameters.Add(tmpFreqTypeParam)
            mDBCommand.Parameters.Add(tmpOutParam)
            mDBCommand.Parameters.Add(tmpErrCode)
            mDBCommand.Parameters.Add(tmpErrMesg)
            mDBCommand.ExecuteNonQuery()

            If strDbPlatform.ToUpper = "ORACLE" Then
                tmpErrCode = mDBCommand.Parameters.Item("ERRCODE")
                If tmpErrCode.Value = 0 Then
                    tmpOutParam = mDBCommand.Parameters.Item("OUTPUT_VAL")
                Else
                    tmpErrMesg = mDBCommand.Parameters.Item("ERRMESG")
                    objstatus.bStatus = False
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest  : GetProcOutput : Getting Error Try to execute the procedure " & tmpErrMesg.Value.string, strUserid)
                    Return objstatus
                End If
            Else
                tmpErrCode = mDBCommand.Parameters.Item("ERRCODE")
                If tmpErrCode.Value = 0 Then
                    tmpOutParam = mDBCommand.Parameters.Item("OUTPUT_VAL")
                Else
                    tmpErrMesg = mDBCommand.Parameters.Item("ERRMESG")
                    objstatus.bStatus = False
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest  : GetProcOutput : Getting Error Try to execute the procedure " & tmpErrMesg.Value.string, strUserid)
                    Return objstatus
                End If
            End If

            strOutValue = tmpOutParam.Value

            If strOutValue = String.Empty Then
                objstatus.bStatus = False
                objstatus.strErrDescription = "Procedure For Dynamic Parameter Returns Null"
                Return objstatus
            End If

            objstatus.bStatus = True
            objstatus.objReturn = strOutValue
            Return objstatus
        Catch ex As Exception
            objstatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest  : GetProcOutput : " & ex.Message & ex.StackTrace, strUserid)
            Return objstatus
        Finally
            objstatus = Nothing
        End Try
    End Function
    Function GetScheduleInfo(ByVal intRequestID As Integer, ByVal strUserid As String)
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim dsScheduleinfo As New DataSet
        Dim strSql As String
        Try


            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & objStatus.strErrDescription, strUserid)
            End If
            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "select a.FREQ_TYPE,b.RUN_DATE from sc_job_schedule_info a,sc_schedule_request b where a.SCHEDULEID=b.SCHEDULEID And b.SCREQUESTID = " & intRequestID
            Else
                strSql = "select a.FREQ_TYPE,b.RUN_DATE from sc_job_schedule_info a,sc_schedule_request b where a.SCHEDULEID=b.SCHEDULEID And b.SCREQUESTID = " & intRequestID
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            mDBAdapter.SelectCommand = mDBCommand
            mDBAdapter.Fill(dsScheduleinfo)

            objStatus.bStatus = True
            objStatus.objReturn = dsScheduleinfo
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, ex.Message, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            objStatus = Nothing
        End Try
    End Function
    Function IsCompleted(ByVal intRequestID As Integer) As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim strOutcome As String
        Dim objOutcome As Object
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If

                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "Select NEXTJOBORDER from sc_schedule_request where  SCREQUESTID =" & intRequestID
                Else
                    strSql = "Select NEXTJOBORDER from sc_schedule_request where  SCREQUESTID =" & intRequestID
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                objOutcome = mDBCommand.ExecuteScalar

                If objOutcome Is System.DBNull.Value Then
                    strOutcome = "FALSE"
                Else
                    If CInt(objOutcome) = 0 Then
                        strOutcome = "TRUE"
                    Else
                        strOutcome = "FALSE"
                    End If
                End If

                objStatus.objReturn = strOutcome
                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & " Error While try to Update the ", strUserid)
            End If
        Catch ex As Exception
            objStatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UpdateReportStatus : ScheduleID:" & intScheduleId & ex.Message & ex.StackTrace, strUserid)
            Return objStatus
        End Try
    End Function
    Function GetJobOutcome(ByVal iHistoryId As Integer) As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim strOutcome As String
        Dim objOutcome As Object

        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If

                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "Select upper(OUTCOME) from sc_job_history where hist_id=" & iHistoryId
                Else
                    strSql = "Select upper(OUTCOME) from sc_job_history where hist_id=" & iHistoryId
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                objOutcome = mDBCommand.ExecuteScalar
                If objOutcome Is System.DBNull.Value Then
                    strOutcome = "FALSE"
                Else
                    If objOutcome.ToString.ToUpper.Equals("COMPLETED SUCCESSFULLY") Then
                        strOutcome = "TRUE"
                    Else
                        strOutcome = "FALSE"
                    End If
                End If

                objStatus.objReturn = strOutcome
                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & " Error While try to Update the History Outcome", strUserid)
            End If
        Catch ex As Exception
            objStatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UpdateReportStatus : ScheduleID:" & intScheduleId & ex.Message & ex.StackTrace, strUserid)
            Return objStatus
        End Try
    End Function

    ' Event Handler for Process.Exited Event
    Function UpdateJobHistory(ByVal iHistoryId As Integer, ByVal strStatus As String, ByVal strOutFilename As String, ByVal strLogFilename As String, ByVal lngDuration As Long, ByVal strUserId As String, ByVal intRequestID As Integer)
        'update the job history 
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim dtNextRun As Date
        Dim timeNextRun As String
        Dim dtTmpDate As String
        Dim dtLastDate As String
        Dim dtModifyDate As String
        Dim strUpdateMsg As String

        Try
            If strOutFilename <> "" Then
                strUpdateMsg = strUpdateMsg & ",output_filename='" & strOutFilename & "'"
            End If
            If strLogFilename <> "" Then
                strUpdateMsg = strUpdateMsg & ",log_filename='" & strLogFilename & "'"
            End If
            If lngDuration <> 0 Then
                strUpdateMsg = strUpdateMsg & ",run_duration=" & lngDuration
            End If
            If intRequestID <> 0 Then
                strUpdateMsg = strUpdateMsg & ",SCREQUESTID = " & intRequestID & ""
            End If

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "update SC_Job_History set message='" & strStatus & "'" & strUpdateMsg & " where hist_id=" & iHistoryId
                Else
                    strSql = "update SC_Job_History with (UPDLOCK) set message='" & strStatus & "'" & strUpdateMsg & " where hist_id=" & iHistoryId
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBCommand.ExecuteNonQuery()

                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserId)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "UpdateJobHistory:HistoryId" & iHistoryId & ex.Message & ex.StackTrace & "StrSQl" & strSql, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            UpdateJobHistory = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function UpdateJobHistoryProcessId(ByVal iHistoryId As Integer, ByVal intProcessId As Integer) As MQSCommonObj.MQSStatus
        'update the job history 
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim strUpdateMsg As String
        Try
            If intProcessId <> 0 Then
                strUpdateMsg = strUpdateMsg & " PROCESSID = " & intProcessId & ""
            End If
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "update SC_Job_History set " & strUpdateMsg & " where hist_id=" & iHistoryId
                Else
                    strSql = "update SC_Job_History with (UPDLOCK) set " & strUpdateMsg & " where hist_id=" & iHistoryId
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBCommand.ExecuteNonQuery()

                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "UpdateJobHistory:HistoryId" & iHistoryId & ex.Message & ex.StackTrace & "StrSQl" & strSql, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            UpdateJobHistoryProcessId = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function UpdateScheduleRequest(ByVal strStatus As String, Optional ByVal intHistoryId As Integer = 0) As MQSCommonObj.MQSStatus
        'update the status for Scheduler Request
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim dtNextRun As Date
        Dim timeNextRun As String
        Dim dtTmpDate As String
        Dim dtLastDate As String
        Dim dtModifyDate As String
        Dim strUpdateMsg As String
        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    If intHistoryId <> 0 Then
                        strSql = "update SC_SCHEDULE_REQUEST set status='" & strStatus.ToUpper & "',HIST_ID=" & intHistoryId.ToString & " where SCREQUESTID=" & intRequestID
                    Else
                        strSql = "update SC_SCHEDULE_REQUEST set status='" & strStatus.ToUpper & "' where SCREQUESTID=" & intRequestID
                    End If
                Else
                    If intHistoryId <> 0 Then
                        strSql = "update SC_SCHEDULE_REQUEST set status='" & strStatus.ToUpper & "',HIST_ID=" & intHistoryId.ToString & " where SCREQUESTID=" & intRequestID
                    Else
                        strSql = "update SC_SCHEDULE_REQUEST set status='" & strStatus.ToUpper & "' where SCREQUESTID=" & intRequestID
                    End If
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBCommand.ExecuteNonQuery()

                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "UpdateScheduleRequest:RequestId" & intRequestID & ex.Message & ex.StackTrace & "strSql" & strSql, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            UpdateScheduleRequest = objStatus
            objStatus = Nothing
        End Try
    End Function
    Public Function SetNewScheduleRequest()
        'To get the next schedule Request Time
        Dim dsScheduleDetails As New DataSet
        Dim objStatus As MQSCommonObj.MQSStatus

        Dim dtStartDate As Date
        Dim strStartTime As String
        Dim dtStartDateTime As DateTime
        Dim dtEndDate As Date

        Dim strFreType As String
        Dim intFreInterval As Integer
        Dim intFreRelativeInterval As Integer
        Dim intFreRecurrenceInverval As Integer

        Dim intIsRepeatable As Integer
        Dim intTimeInterval As Integer
        Dim intRecurrenceDuration As Integer
        Dim intDuration As Long

        Dim intDay As Int16
        Dim intMonth As Int16
        Dim blnGet As Boolean
        Dim intDayPower As Integer
        Dim intMonthPower As Integer
        Dim intDayCount As Integer
        Dim intMonthCount As Int16
        Dim dtTempDateTime As DateTime
        Dim dtSecTempDateTime As DateTime
        Dim intGroupId As Integer

        Dim dtNextRunDateTime As DateTime
        Dim dtRunDate As DateTime
        Dim dtRunDateTime As DateTime

        Dim blnRecursive As Boolean

        Dim intMonthDiff As Long
        Dim intMonthDiv As Long

        Dim intDayDiff As Long
        Dim intDaysDiv As Long


        Try
            'get the Scheduler details
            objStatus = RetrieveScheduleDetails(intScheduleId, strUserid)
            If objStatus.bStatus = False Then
                SetNewScheduleRequest = objStatus
                Exit Function
            End If
            dsScheduleDetails = objStatus.objReturn
            If dsScheduleDetails.Tables(0).Rows.Count > 0 Then

                intGroupId = dsScheduleDetails.Tables(0).Rows(0)("GROUPID")
                strStartTime = dsScheduleDetails.Tables(0).Rows(0)("ACTIVE_START_TIME").ToString
                dtStartDate = dsScheduleDetails.Tables(0).Rows(0)("ACTIVE_START_DATE")
                dtStartDateTime = CDate(Format(dtStartDate, "dd/MM/yyyy") & " " & strStartTime)
                strFreType = dsScheduleDetails.Tables(0).Rows(0)("FREQ_TYPE")
                intFreInterval = dsScheduleDetails.Tables(0).Rows(0)("FREQ_INTERVAL")
                intFreRecurrenceInverval = dsScheduleDetails.Tables(0).Rows(0)("FREQ_RECURRENCE_FACTOR")
                intFreRelativeInterval = dsScheduleDetails.Tables(0).Rows(0)("FREQ_RELATIVE_INTERVAL")

                'if end date is null set the end date to 100 years
                If dsScheduleDetails.Tables(0).Rows(0)("ACTIVE_END_DATE") Is DBNull.Value Then
                    'set max value
                    dtEndDate = Now
                    dtEndDate = dtEndDate.AddYears(100)
                Else
                    dtEndDate = dsScheduleDetails.Tables(0).Rows(0)("ACTIVE_END_DATE")
                End If

                'if End date is less than today's date
                If Not (dtEndDate.AddDays(1) < Now) Then

                    'if repetable case
                    If dsScheduleDetails.Tables(0).Rows(0)("ISREPEATABLE") = 1 Then

                        intTimeInterval = dsScheduleDetails.Tables(0).Rows(0)("TIMEINTERVAL")
                        intRecurrenceDuration = dsScheduleDetails.Tables(0).Rows(0)("REC_DURATION")

                        dtRunDateTime = CDate(Format(Now, "dd/MM/yyyy") & " " & strStartTime)
                        'get the duration i.e present time - schedule start time
                        If dtRunDateTime > Now Then
                            intDuration = DateDiff(DateInterval.Minute, dtRunDateTime.AddDays(-1), Now)
                        Else
                            intDuration = DateDiff(DateInterval.Minute, dtRunDateTime, Now)
                        End If


                        'if duratation is less than recurrence duration
                        If intDuration < intRecurrenceDuration Then
                            'calculate the duration time
                            intDuration = intDuration + intTimeInterval - (intDuration Mod intTimeInterval)
                            If intDuration < intRecurrenceDuration Then
                                blnRecursive = True
                            End If
                            If Not dsScheduleDetails.Tables(0).Rows(0)("NEXT_RUN_DATE") Is DBNull.Value Then
                                dtNextRunDateTime = dsScheduleDetails.Tables(0).Rows(0)("NEXT_RUN_DATE")
                                dtNextRunDateTime = CDate(Format(dtNextRunDateTime, "dd/MM/yyyy") & " " & strStartTime)

                                'add the duration time
                                dtNextRunDateTime = dtNextRunDateTime.AddMinutes(intDuration)
                                'if grater then presnet time schedule the request
                                If dtNextRunDateTime > Now Then
                                    If dtNextRunDateTime < dtEndDate.AddDays(1) Then
                                        InsertScheduleRequest(intScheduleId, intGroupId, "N", dtNextRunDateTime.ToShortDateString, dtNextRunDateTime.ToLongTimeString, strAppServerName, strUserid)
                                        UpdateScheduleInfo(intScheduleId, dtNextRunDateTime, dtNextRunDateTime.ToLongTimeString, strUserid)
                                    Else
                                        UpdateScheduleInfo(intScheduleId, Nothing, "", strUserid)
                                    End If
                                    objStatus.bStatus = True
                                    SetNewScheduleRequest = objStatus
                                    Exit Function
                                End If
                            End If
                        End If
                    End If
                    dtNextRunDateTime = Nothing
                    If strFreType = freq_type.Once Then
                        'update next run time to nothing
                        UpdateScheduleInfo(intScheduleId, Nothing, "", strUserid)
                        Exit Function
                    ElseIf strFreType = freq_type.Daily Then
                        'get the days difference count
                        intDayDiff = DateDiff(DateInterval.Day, dtStartDateTime, Now)

                        If intFreInterval = 0 Then
                            'set the next run date to today's date
                            dtNextRunDateTime = CDate(Now.ToShortDateString & " " & strStartTime)
                        Else
                            'set the next run date to nearest date
                            dtNextRunDateTime = dtStartDateTime.AddDays(intDayDiff - (intDayDiff Mod intFreInterval))
                        End If
                        If blnRecursive = True Then
                            'add the duration interval
                            dtNextRunDateTime.AddMinutes(intDuration)
                        End If
                        'while next run date is grater then today's date 
                        While dtNextRunDateTime < Now
                            If intFreInterval = 0 Then
                                'if interval is 0 add one day
                                dtNextRunDateTime = dtNextRunDateTime.AddDays(1)
                            Else
                                'if interval is grater than 0 add the interval
                                'case every 2 days
                                dtNextRunDateTime = dtNextRunDateTime.AddDays(intFreInterval)
                            End If
                        End While
                    ElseIf strFreType = freq_type.Weekly Then
                        'get the day's difference count 
                        intDayDiff = DateDiff(DateInterval.Day, dtStartDateTime, Now)

                        If intFreRecurrenceInverval = 0 Then
                            'set the next run date to nearest week
                            dtNextRunDateTime = dtStartDateTime.AddDays(intDayDiff - (intDayDiff Mod 7))
                        Else
                            'set the next run date to nearest week
                            dtNextRunDateTime = dtStartDateTime.AddDays(intDayDiff - (intDayDiff Mod (7 * intFreRecurrenceInverval)))
                        End If

                        If blnRecursive = True Then
                            dtNextRunDateTime.AddMinutes(intDuration)
                        End If

                        intDay = dtStartDateTime.DayOfWeek
                        intDayCount = intDay
                        intDayCount = 0
                        'sunday day of the week
                        dtNextRunDateTime = dtNextRunDateTime.AddDays(-intDay)
                        blnGet = False

                        While Not blnGet
                            dtTempDateTime = dtNextRunDateTime
                            While intDayCount <= 6
                                If intFreInterval And (2 ^ intDayCount) Then
                                    dtTempDateTime = dtNextRunDateTime
                                    dtTempDateTime = dtTempDateTime.AddDays(intDayCount)
                                    If dtTempDateTime > Now Then
                                        Exit While
                                    End If
                                End If
                                intDayCount = intDayCount + 1
                            End While
                            intDayCount = 0
                            If dtTempDateTime > Now Then
                                blnGet = True
                                dtNextRunDateTime = dtTempDateTime
                            Else
                                If dtNextRunDateTime < Now Then
                                    If intFreRecurrenceInverval = 0 Then
                                        dtNextRunDateTime = dtNextRunDateTime.AddDays(7)
                                    Else
                                        dtNextRunDateTime = dtNextRunDateTime.AddDays(intFreRecurrenceInverval * 7)
                                    End If

                                    intDay = 0
                                End If
                            End If
                        End While
                    ElseIf strFreType = freq_type.Monthly Then
                        'in case of x day of the month
                        'Get the months difference
                        intMonthDiff = DateDiff(DateInterval.Month, dtStartDateTime, Now)

                        'set the next rundate to nearest date
                        dtNextRunDateTime = dtStartDateTime.AddMonths(intMonthDiff - (intMonthDiff Mod 12))

                        'goto jan 1st
                        dtNextRunDateTime = dtNextRunDateTime.AddMonths(-dtNextRunDateTime.Month + 1)

                        'first day of the month
                        dtNextRunDateTime = dtNextRunDateTime.AddDays(-1 * (dtNextRunDateTime.Day - 1))

                        If blnRecursive = True Then
                            dtNextRunDateTime.AddMinutes(intDuration)
                        End If

                        'intMonth = dtStartDateTime.Month
                        intMonth = 1
                        intMonthCount = intMonth
                        'dtNextRunDateTime = dtStartDateTime
                        blnGet = False

                        While Not blnGet
                            'set the next run time in temporary variable
                            dtTempDateTime = dtNextRunDateTime
                            While intMonthCount <= 12
                                dtTempDateTime = dtNextRunDateTime
                                'if schedule in the month
                                If intFreRecurrenceInverval And (2 ^ (intMonthCount - 1)) Then
                                    dtTempDateTime = dtTempDateTime.AddMonths(intMonthCount - intMonth)
                                    'set the next run date in second temp variable
                                    dtSecTempDateTime = dtTempDateTime
                                    'add the frequency interval days to second temp date
                                    dtSecTempDateTime = dtSecTempDateTime.AddDays(intFreInterval - 1)
                                    'if given day is avialbe in the selected month
                                    If dtSecTempDateTime.Month = dtTempDateTime.Month Then
                                        If dtSecTempDateTime > Now Then
                                            dtTempDateTime = dtSecTempDateTime
                                            blnGet = True
                                            Exit While
                                        End If
                                    End If
                                End If
                                intMonthCount = intMonthCount + 1
                            End While
                            'if get the next run date and time
                            If dtTempDateTime > Now And blnGet = True Then
                                dtNextRunDateTime = dtTempDateTime
                            Else
                                'add 12 months to the next run date
                                dtNextRunDateTime = dtNextRunDateTime.AddMonths(12)
                                intMonth = 1
                                intMonthCount = 1
                            End If
                        End While
                    ElseIf strFreType = freq_type.Monthly_relative_freq Then
                        'in case of first second.. 
                        'get the month differecne
                        intMonthDiff = DateDiff(DateInterval.Month, dtStartDateTime, Now)
                        'go to the nearest next date
                        dtNextRunDateTime = dtStartDateTime.AddMonths(intMonthDiff - (intMonthDiff Mod 12))
                        'goto jan 
                        dtNextRunDateTime = dtNextRunDateTime.AddMonths(-dtNextRunDateTime.Month + 1)

                        If blnRecursive = True Then
                            dtNextRunDateTime.AddMinutes(intDuration)
                        End If

                        Dim intWeekDay As Int16
                        Dim intDaystoadd As Int16
                        'intMonth = dtStartDateTime.Month
                        intMonth = 1
                        intMonthCount = intMonth
                        'dtNextRunDateTime = dtStartDateTime
                        blnGet = False
                        While Not blnGet
                            'set the next run date to temp
                            dtTempDateTime = dtNextRunDateTime
                            While intMonthCount <= 12
                                dtTempDateTime = dtNextRunDateTime
                                'check whether the month is selected or not 
                                If intFreRecurrenceInverval And (2 ^ (intMonthCount - 1)) Then
                                    dtTempDateTime = dtTempDateTime.AddMonths(intMonthCount - intMonth)
                                    'first to fourth weeks
                                    If intFreRelativeInterval <= 4 Then
                                        'get the first day of month
                                        dtTempDateTime = dtTempDateTime.AddDays(-1 * (dtTempDateTime.Day - 1))

                                        intWeekDay = dtTempDateTime.DayOfWeek + 1

                                        Dim TempintFreInterval As Int16

                                        '7 mod 7 is zero but we have to add one 
                                        TempintFreInterval = Math.Log(intFreInterval, 2) + 1

                                        'intFreInterval = Math.Log(intFreInterval, 2) + 1

                                        intDaystoadd = 7 - (intWeekDay - TempintFreInterval)

                                        intDaystoadd = intDaystoadd Mod 7

                                        dtTempDateTime = dtTempDateTime.AddDays(intDaystoadd)
                                        'go to the exeact week
                                        dtTempDateTime = dtTempDateTime.AddDays((intFreRelativeInterval - 1) * 7)
                                    Else
                                        'for the last week
                                        'get the last day
                                        dtTempDateTime = dtTempDateTime.AddDays(dtTempDateTime.DaysInMonth(dtTempDateTime.Year, dtTempDateTime.Month) - dtTempDateTime.Day)

                                        intWeekDay = dtTempDateTime.DayOfWeek + 1

                                        Dim TempintFreInterval As Int16

                                        TempintFreInterval = Math.Log(intFreInterval, 2) + 1
                                        'intFreInterval = Math.Log(intFreInterval, 2) + 1

                                        intDaystoadd = 7 - (intWeekDay - TempintFreInterval)

                                        intDaystoadd = intDaystoadd Mod 7

                                        If intDaystoadd <> 0 Then
                                            dtTempDateTime = dtTempDateTime.AddDays(intDaystoadd - 7)
                                        End If

                                    End If

                                    If dtTempDateTime > Now Then
                                        Exit While
                                    End If
                                End If
                                intMonthCount = intMonthCount + 1
                            End While
                            If dtTempDateTime > Now Then
                                blnGet = True
                                dtNextRunDateTime = dtTempDateTime
                            Else
                                If dtNextRunDateTime < Now Then
                                    'go to the next year
                                    dtNextRunDateTime = dtNextRunDateTime.AddMonths(12)
                                    intMonth = 1
                                    intMonthCount = 1
                                End If
                            End If
                        End While
                    End If
                    If dtNextRunDateTime < dtEndDate.AddDays(1) Then
                        'insert the next run date into request table
                        objStatus = InsertScheduleRequest(intScheduleId, intGroupId, "N", dtNextRunDateTime.ToShortDateString, dtNextRunDateTime.ToLongTimeString, strAppServerName, strUserid)
                        If objStatus.bStatus = True Then
                            'udate the schedul info with the next run date
                            objStatus = UpdateScheduleInfo(intScheduleId, dtNextRunDateTime, dtNextRunDateTime.ToLongTimeString, strUserid)
                        End If
                    Else
                        'udate the schedul info with the next run date
                        objStatus = UpdateScheduleInfo(intScheduleId, Nothing, "", strUserid)
                    End If
                Else
                    'udate the schedul info with the next run date
                    objStatus = UpdateScheduleInfo(intScheduleId, Nothing, "", strUserid)
                End If
            End If
            SetNewScheduleRequest = objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "SetNewScheduleRequest:ScheduleId" & intScheduleId & ex.Message & ex.StackTrace, strUserid)
            objStatus.bStatus = False
            SetNewScheduleRequest = objStatus
        Finally
            SetNewScheduleRequest = objStatus
            objStatus = Nothing
        End Try
    End Function
    Private Function RetrieveScheduleDetails(ByVal intScheduleId As Integer, ByVal strUserid As String) As MQSCommonObj.MQSStatus
        'get the given scheduleid details
        Dim strSql As String
        Dim dsScheduleDetails As New DataSet
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim objAdapt As Object
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select SCHEDULEID, GROUPID, SCHDESC, NAME, STATUS, FREQ_TYPE, FREQ_INTERVAL, FREQ_RELATIVE_INTERVAL, FREQ_RECURRENCE_FACTOR, ACTIVE_START_DATE, ACTIVE_START_TIME, ACTIVE_END_DATE, ACTIVE_END_TIME, LAST_RUN_DATE, LAST_RUN_TIME, LAST_RUN_OUTCOME, LAST_RUN_DURATION, NEXT_RUN_DATE, NEXT_RUN_TIME,  CREATED_BY, CREATED_DATE, MODIFIED_BY, MODIFIED_DATE, APPSERVER, ISREPEATABLE, TIMEINTERVAL, REC_DURATION from SC_JOB_SCHEDULE_INFO where scheduleid = " & intScheduleId
                Else
                    strSql = "select SCHEDULEID, GROUPID, SCHDESC, NAME, STATUS, FREQ_TYPE, FREQ_INTERVAL, FREQ_RELATIVE_INTERVAL, FREQ_RECURRENCE_FACTOR, ACTIVE_START_DATE, ACTIVE_START_TIME, ACTIVE_END_DATE, ACTIVE_END_TIME, LAST_RUN_DATE, LAST_RUN_TIME, LAST_RUN_OUTCOME, LAST_RUN_DURATION, NEXT_RUN_DATE, NEXT_RUN_TIME, CREATED_BY, CREATED_DATE, MODIFIED_BY, MODIFIED_DATE, APPSERVER, ISREPEATABLE, TIMEINTERVAL, REC_DURATION from SC_JOB_SCHEDULE_INFO where scheduleid = " & intScheduleId
                End If
                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBAdapter.SelectCommand = mDBCommand
                mDBAdapter.Fill(dsScheduleDetails)
                objStatus.bStatus = True
                objStatus.objReturn = dsScheduleDetails
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "RetrieveScheduleDetails:ScheduleId" & intScheduleId & ex.Message & ex.StackTrace, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            RetrieveScheduleDetails = objStatus
            objStatus = Nothing
        End Try
    End Function
    Private Function InsertScheduleRequest(ByVal iScheduleId As Integer, ByVal iGroupId As Integer, ByVal strStatus As String, ByVal rundate As Date, ByVal runtime As DateTime, ByVal strAppServer As String, ByVal strUserId As String) As MQSCommonObj.MQSStatus
        'insert the new schedule request into request table
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim strRundate As String
        Dim strRuntime As String
        Dim dtTmpDate As String
        Try
            If strDbPlatform.ToUpper = "ORACLE" Then
                dtTmpDate = rundate.ToShortDateString & " " & rundate.ToLongTimeString  'Format(rundate, "dd/MM/yyyy HH:mm:ss PM")
                dtTmpDate = "to_date('" & dtTmpDate & "','dd/mm/yyyy hh:mi:ss pm')"
            ElseIf strDbPlatform.ToUpper = "SQLSERVER" Then
                dtTmpDate = "'" & Format(rundate.Date, "yyyy-MM-dd").ToString & " " & rundate.ToLongTimeString & "'"  'Format(rundate, "MM/dd/yyyy HH:mm:ss PM") & "'"
            End If
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "insert into SC_SCHEDULE_REQUEST(SCHEDULEID, GROUPID, STATUS, APPSERVER, RUN_DATE, RUN_TIME,USERID) values(" & iScheduleId & "," & iGroupId & "," & "'" & strStatus.ToUpper & "','" & strAppServer.ToUpper & "'," & dtTmpDate & ",'" & runtime & "','" & strUserId & "')"
                Else
                    strSql = "insert into SC_SCHEDULE_REQUEST(SCHEDULEID, GROUPID, STATUS, APPSERVER, RUN_DATE, RUN_TIME,USERID) values(" & iScheduleId & "," & iGroupId & "," & "'" & strStatus.ToUpper & "','" & strAppServer.ToUpper & "'," & dtTmpDate & ",'" & runtime & "','" & strUserId & "')"
                End If
                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBCommand.ExecuteNonQuery()

                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserId)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "InsertScheduleRequest:ScheduleId" & iScheduleId & ex.Message & ex.StackTrace & "strSql" & strSql, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            InsertScheduleRequest = objStatus
            objStatus = Nothing
        End Try
    End Function
    Public Function checkSchedulerStatus(ByRef blnStatus As Boolean) As MQSCommonObj.MQSStatus
        'if job is in recursive then check any job is running with the same schedule id 
        'then send blnStatus as true
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim intCount As Integer
        Dim intIsSingleTon As Integer
        Dim intSingletonCount As Integer
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select count(SCREQUESTID) from SC_SCHEDULE_REQUEST where STATUS='R' AND SCHEDULEID=" & intScheduleId
                Else
                    strSql = "select count(SCREQUESTID) from SC_SCHEDULE_REQUEST where STATUS='R' AND SCHEDULEID=" & intScheduleId
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                intCount = mDBCommand.ExecuteScalar()

                If intCount > 0 Then
                    blnStatus = False
                    UpdateScheduleRequest("C")
                Else
                    blnStatus = True
                End If

                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch e As OracleException
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "CheckSchedulerStatus:ScheduleId" & intScheduleId & e.Message & e.StackTrace, strUserid)
            objStatus.strErrDescription = e.Message
            objStatus.bStatus = False
            Return objStatus
        Catch e As SqlClient.SqlException
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "CheckSchedulerStatus:ScheduleId" & intScheduleId & e.Message & e.StackTrace, strUserid)
            objStatus.strErrDescription = e.Message
            objStatus.bStatus = False
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "CheckSchedulerStatus:ScheduleId" & intScheduleId & ex.Message & ex.StackTrace, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            checkSchedulerStatus = objStatus
            objStatus = Nothing
        End Try
    End Function
    Private Function checkSchedulerSingleTonStatus(ByRef blnSingleTonStatus As Boolean) As MQSCommonObj.MQSStatus
        'if job is single ton job check whether any job is running state
        'if yes send status as true
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim intCount As Integer
        Dim intIsSingleTon As Integer
        Dim intSingletonCount As Integer
        Try
            blnSingleTonStatus = False
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select count(SCREQUESTID) from SC_SCHEDULE_REQUEST where STATUS='R' AND GROUPID=" & intGroupId.ToString
                Else
                    strSql = "select count(SCREQUESTID) from SC_SCHEDULE_REQUEST where STATUS='R' AND GROUPID=" & intGroupId.ToString
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                intSingletonCount = mDBCommand.ExecuteScalar()

                If intSingletonCount > 0 Then
                    blnSingleTonStatus = True
                Else
                    blnSingleTonStatus = False
                End If

                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If

        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "checkSchedulerSingleTonStatus:ScheduleId" & ex.Message & ex.StackTrace, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            checkSchedulerSingleTonStatus = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function RetrieveJobDetails(ByVal intProcessId As Integer) As MQSCommonObj.MQSStatus
        'get the job details for the given process id this will called on on process exist
        Dim strSql As String
        Dim dsJobCmd As New DataSet
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select a.Commandtoexecute,a.jobid,b.name,c.output_file_name,d.name jobtype,c.command,e.RUN_DATE,e.RUN_TIME, "
                    strSql = strSql & "c.cmdexec_success_code,c.name jobname,c.process,A.SCHEDULEID,B.CREATED_BY,e.HIST_ID,F.SCREQUESTID  "
                    strSql = strSql & "from sc_scheduled_jobs a,sc_job_schedule_info b,sc_m_job c,sc_m_job_type d,SC_Job_History e,SC_SCHEDULE_REQUEST f "
                    strSql = strSql & "where  e.PROCESSID=" & intProcessId & " AND F.APPSERVER='" & strAppServerName & "' AND f.STATUS='R'  AND a.scheduleid=b.scheduleid and a.jobid=c.jobid and d.jobtypeid=c.typeid and a.scheduleid=f.SCHEDULEID and a.scheduleid=e.SCHEDULE_ID"
                Else

                    strSql = "select a.Commandtoexecute,a.jobid,b.name,c.output_file_name,d.name jobtype,c.command,e.RUN_DATE,e.RUN_TIME, "
                    strSql = strSql & "c.cmdexec_success_code,c.name jobname,c.process,A.SCHEDULEID,B.CREATED_BY,e.HIST_ID,F.SCREQUESTID "
                    strSql = strSql & "from sc_scheduled_jobs a,sc_job_schedule_info b,sc_m_job c,sc_m_job_type d,SC_Job_History e,SC_SCHEDULE_REQUEST f "
                    strSql = strSql & "where  e.PROCESSID=" & intProcessId & " AND F.APPSERVER='" & strAppServerName & "' AND f.STATUS='R'  AND a.scheduleid=b.scheduleid and a.jobid=c.jobid and d.jobtypeid=c.typeid and a.scheduleid=f.SCHEDULEID and a.scheduleid=e.SCHEDULE_ID"
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBAdapter.SelectCommand = mDBCommand
                mDBAdapter.Fill(dsJobCmd)

                objStatus.bStatus = True
                objStatus.objReturn = dsJobCmd
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch e As OracleException
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "RetrieveJobDetails:Processid" & intProcessId & e.Message & e.StackTrace, "PORT")
            objStatus.strErrDescription = e.Message
            objStatus.bStatus = False
            Return objStatus
        Catch e As SqlClient.SqlException
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "RetrieveJobDetails:Processid" & intProcessId & e.Message & e.StackTrace, strUserid)
            objStatus.strErrDescription = e.Message
            objStatus.bStatus = False
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "RetrieveJobDetails:Processid" & intProcessId & ex.Message & ex.StackTrace & "strSQL" & strSql, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            RetrieveJobDetails = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function CommitTransaction()
        Try
            If Not mDBTransaction Is Nothing Then
                mDBTransaction.Commit()
                mDBConnection.Close()
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "CommitTransaction:" & ex.Message & ex.StackTrace, "PORT")
            If Not mDBConnection Is Nothing Then
                If mDBConnection.State = ConnectionState.Open Then
                    mDBConnection.Close()
                End If
            End If
        Finally
            If mDBConnection.State = ConnectionState.Open Then
                mDBConnection.Close()
            End If
            mDBConnection = Nothing
            mDBCommand = Nothing
            mDBTransaction = Nothing
        End Try
    End Function
    Function abortTransaction()
        Try
            If Not mDBTransaction Is Nothing Then
                mDBTransaction.Rollback()
                mDBConnection.Close()
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "AboutTransaction:" & ex.Message & ex.StackTrace, "PORT")
            If Not mDBConnection Is Nothing Then
                If mDBConnection.State = ConnectionState.Open Then
                    mDBConnection.Close()
                End If
            End If
        Finally
            If mDBConnection.State = ConnectionState.Open Then
                mDBConnection.Close()
            End If
            mDBConnection = Nothing
            mDBCommand = Nothing
            mDBTransaction = Nothing
        End Try
    End Function
    Function openConnection() As MQSCommonObj.MQSStatus
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try
            If mDBConnection Is Nothing Then
                If strDbPlatform.ToUpper = "ORACLE" Then
                    mDBConnection = New OracleConnection(strConnection)
                    mDBCommand = New OracleCommand
                    mDBAdapter = New OracleDataAdapter
                ElseIf strDbPlatform.ToUpper = "SQLSERVER" Then
                    mDBConnection = New SqlConnection(strConnection)
                    mDBCommand = New SqlCommand
                    mDBAdapter = New SqlDataAdapter
                End If
            ElseIf mDBConnection.State <> ConnectionState.Open Then
                If strDbPlatform.ToUpper = "ORACLE" Then
                    mDBConnection = New OracleConnection(strConnection)
                    mDBCommand = New OracleCommand
                    mDBAdapter = New OracleDataAdapter
                ElseIf strDbPlatform.ToUpper = "SQLSERVER" Then
                    mDBConnection = New SqlConnection(strConnection)
                    mDBCommand = New SqlCommand
                    mDBAdapter = New SqlDataAdapter
                End If
            End If
            mDBConnection.Open()
            mDBCommand.Connection = mDBConnection
            mDBAdapter.SelectCommand = mDBCommand
            mDBTransaction = mDBConnection.BeginTransaction
            mDBCommand.Transaction = mDBTransaction
            objStatus.bStatus = True
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "OpenConnection: " & ex.Message & ex.StackTrace, "PORT")
            objStatus.bStatus = False
        Finally
            openConnection = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function UpdateScheduleInfoStatus(ByVal strStatus As String, Optional ByVal intDuration As Integer = -1, Optional ByVal dtstartdatetime As DateTime = #12/12/2006#) As MQSCommonObj.MQSStatus
        'updates the scheduler info status 
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim dtTmpDate As String
        Dim strDuration As String = ""
        Dim dtModifyDate As String
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If
                If strStatus = "Completed" Then
                    strStatus = " CASE WHEN NEXT_RUN_DATE is null THEN 'Completed' ELSE 'Scheduled' END "
                Else
                    strStatus = "'" & strStatus & "'"
                End If
                If intDuration <> -1 Then
                    If strDbPlatform.ToUpper = "ORACLE" Then
                        strDuration = " ,last_run_date=to_date('" & dtstartdatetime & "','dd/mm/yyyy hh:mi:ss PM'),last_run_time='" & dtstartdatetime.ToLongTimeString & "',last_run_duration=" & intDuration.ToString
                    Else
                        strDuration = " ,last_run_date='" & Format(dtstartdatetime.Today, "yyyy-MM-dd").ToString & " " & dtstartdatetime.ToLongTimeString & "',last_run_time='" & dtstartdatetime.ToLongTimeString & "',last_run_duration=" & intDuration.ToString
                    End If

                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    dtModifyDate = Now.ToShortDateString & " " & Now.ToLongTimeString 'Format(Now, "dd/MM/yyyy HH:mm:ss PM")
                    dtModifyDate = "to_date('" & dtModifyDate & "','dd/mm/yyyy hh:mi:ss PM')"
                    strSql = "update SC_Job_Schedule_Info set status=" & strStatus & ",modified_date=" & dtModifyDate & " " & strDuration & " where Scheduleid=" & intScheduleId
                Else
                    dtModifyDate = "'" & Format(Now.Today, "yyyy-MM-dd").ToString & " " & Now.ToLongTimeString & "'"  'Format(Now, "MM/dd/yyyy HH:mm:ss PM") & "'"
                    strSql = "update SC_Job_Schedule_Info set status=" & strStatus & ",modified_date=" & dtModifyDate & " " & strDuration & " where Scheduleid=" & intScheduleId
                End If
                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBCommand.ExecuteNonQuery()

                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "UpadateScheduleInfo:ScheduleId:" & strSql & intScheduleId & ex.Message & ex.StackTrace, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            UpdateScheduleInfoStatus = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function UpdateScheduleInfo(ByVal intSchedulerId As Integer, ByVal dtNextRundate As Date, ByVal strNextRunTime As String, ByVal strUserId As String) As MQSCommonObj.MQSStatus
        'updates the scheduler info status 
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim dtTmpDate As String
        Dim dtModifyDate As String
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If strDbPlatform.ToUpper = "ORACLE" Then
                    dtModifyDate = Now.ToShortDateString & " " & Now.ToLongTimeString 'Format(Now, "dd/MM/yyyy HH:mm:ss PM")
                    dtModifyDate = "to_date('" & dtModifyDate & "','dd/mm/yyyy hh:mi:ss PM')"
                    If dtNextRundate <> "#12:00:00 AM#" Then
                        dtTmpDate = dtNextRundate.ToShortDateString & " " & dtNextRundate.ToLongTimeString 'Format(dtNextRundate, "dd/MM/yyyy HH:mm:ss PM")
                        dtTmpDate = "to_date('" & dtTmpDate & "','dd/mm/yyyy hh:mi:ss pm')"
                        strSql = "update SC_Job_Schedule_Info set status='Running',modified_date=" & dtModifyDate & ",next_run_date=" & dtTmpDate & ",next_run_time='" & strNextRunTime & "' where Scheduleid=" & intSchedulerId
                    Else
                        strSql = "update SC_Job_Schedule_Info set status='Running',next_run_date=null,next_run_time=null,modified_date=" & dtModifyDate & " where Scheduleid=" & intSchedulerId
                    End If
                Else
                    dtModifyDate = "'" & Format(Now.Today, "yyyy-MM-dd").ToString & " " & Now.ToLongTimeString & "'"  'Format(Now, "MM/dd/yyyy HH:mm:ss PM") & "'"
                    If dtNextRundate <> "#12:00:00 AM#" Then
                        dtTmpDate = "'" & Format(dtNextRundate.Date, "yyyy-MM-dd").ToString & " " & dtNextRundate.ToLongTimeString & "'"
                        strSql = "update SC_Job_Schedule_Info set status='Running',modified_date=" & dtModifyDate & ",next_run_date=" & dtTmpDate & ",next_run_time='" & strNextRunTime & "' where Scheduleid=" & intSchedulerId
                    Else
                        strSql = "update SC_Job_Schedule_Info set status='Running',next_run_date=null,next_run_time=null,modified_date=" & dtModifyDate & " where Scheduleid=" & intSchedulerId
                    End If
                End If
                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBCommand.ExecuteNonQuery()

                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserId)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "UpadateScheduleInfo:ScheduleId:" & strSql & intSchedulerId & ex.Message & ex.StackTrace, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            'trans.Rollback()
            Return objStatus
        Finally
            UpdateScheduleInfo = objStatus
            objStatus = Nothing
        End Try
    End Function
    Private Sub UpdateStatusAsStop()
        Dim objStatus As New MQSCommonObj.MQSStatus
        objStatus = UpdateScheduleRequest("C")
        If objStatus.bStatus = True Then
            CommitTransaction()
        End If
        objStatus = UpdateScheduleInfoStatus("Failed To Start")
        If objStatus.bStatus = True Then
            CommitTransaction()
        End If
    End Sub
    Public Function UpdateStatusAsRun() As MQSCommonObj.MQSStatus
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try
            'update the job Request
            objStatus = UpdateScheduleRequest("R", iHistoryId)
            If objStatus.bStatus = False Then
                abortTransaction()
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "StartProcess : Unable Update RequestId:" & intRequestID.ToString, strUserid)
                UpdateStatusAsStop()
                Exit Function
            End If
            objStatus = UpdateScheduleInfoStatus("Running")
            If objStatus.bStatus = False Then
                abortTransaction()
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "StartProcess : Unable Update RequestId:" & intRequestID.ToString, strUserid)
                UpdateStatusAsStop()
                Exit Function
            End If
            CommitTransaction()
            objStatus.bStatus = True
            UpdateStatusAsRun = objStatus

        Catch ex As Exception
            objStatus.bStatus = False
            UpdateStatusAsRun = objStatus
        Finally
            UpdateStatusAsRun = objStatus
            objStatus = Nothing
        End Try
    End Function
    Public Function CheckStartStatus() As MQSCommonObj.MQSStatus
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim blnIsSingleton As Boolean
        Dim blnStatus As Boolean
        Try
            'This method will check if job is in recursive then check any job is running with the same schedule id 
            'then update the currecnt request with 'C'. and set one request for the same.
            objStatus = checkSchedulerStatus(blnStatus)
            If objStatus.bStatus = True Then
                If blnStatus = False Then
                    objStatus = SetNewScheduleRequest()
                    If objStatus.bStatus = False Then
                        CommitTransaction()
                    Else
                        abortTransaction()
                    End If
                    objStatus.bStatus = False
                    Exit Function
                End If
            End If
            'get the Scheduler values
            objStatus = GetJobs()
            If objStatus.bStatus = False Then
                abortTransaction()
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "StartProcess:Unable to get the Command For SchId:" & intScheduleId.ToString, strUserid)
                UpdateStatusAsStop()
                objStatus.bStatus = False
                Exit Function
            End If
            If objStatus.bStatus = True Then
                dsJobs = objStatus.objReturn
                'startthe first job
                If dsJobs Is Nothing Then
                    abortTransaction()
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Start Process:No jobs are Exists for the given Group SchId:" & intScheduleId.ToString, strUserid)
                    'update the group status
                    UpdateStatusAsStop()
                    objStatus.bStatus = False
                    Exit Function
                End If
                If dsJobs.Tables(0).Rows.Count = 0 Then
                    abortTransaction()
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Start Process:No jobs are Exists for the given Group SchId:" & intScheduleId.ToString, strUserid)
                    'update the group status
                    UpdateStatusAsStop()
                    objStatus.bStatus = False
                    Exit Function
                End If
            End If
            If dsJobs.Tables(0).Rows.Count > 0 Then
                objStatus = RetrieveServerName()
                If objStatus.bStatus = False Then
                    abortTransaction()
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Start Process:No Server Details are Exists for the given Group SchId:" & intScheduleId.ToString, strUserid)
                    'update the group status
                    UpdateStatusAsStop()
                    objStatus.bStatus = False
                    Exit Function
                End If
                dsSvrName = objStatus.objReturn
                If dsSvrName Is Nothing Then
                    abortTransaction()
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Start Process:No jobs are Exists for the given Group SchId:" & intScheduleId.ToString, strUserid)
                    'update the group status
                    UpdateStatusAsStop()
                    objStatus.bStatus = False
                    Exit Function
                End If
                If dsSvrName.Tables(0).Rows.Count = 0 Then
                    abortTransaction()
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Start Process:No jobs are Exists for the given Group SchId:" & intScheduleId.ToString, strUserid)
                    'update the group status
                    UpdateStatusAsStop()
                    objStatus.bStatus = False
                    Exit Function
                End If
                'check the job is singleton
                If Not dsJobs.Tables(0).Rows(0).Item("ISSINGLETON") Is System.DBNull.Value Then
                    If dsJobs.Tables(0).Rows(0).Item("ISSINGLETON") = "1" Then
                        'check any other group is running
                        objStatus = checkSchedulerSingleTonStatus(blnIsSingleton)
                        If blnIsSingleton = True Then
                            objStatus.bStatus = False
                            Exit Function
                        End If
                    End If
                End If
            End If
            objStatus.bStatus = True
            CommitTransaction()
        Catch ex As Exception
            abortTransaction()
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "OpenConnection:" & ex.Message & ex.StackTrace, "PORT")
            objStatus.bStatus = False
        Finally
            CheckStartStatus = objStatus
            objStatus = Nothing
        End Try
    End Function
    Public Sub New(ByVal p_strDbPlatform As String, ByVal p_strConnection As String, ByVal p_strAppServerName As String, ByVal p_strSchedulerFilePath As String, ByVal p_strUserid As String, ByVal p_intRequestID As String, ByVal p_intScheduleId As Integer, ByVal p_intGroupId As String, ByVal p_strMsgTempCode As String, ByVal p_strJobOutputType As String, ByVal p_intIsImplicit As Integer, ByVal p_intJobOrder As Integer, ByVal p_strServerIP As String, ByVal p_Rundate As Date)
        Try
            strDbPlatform = p_strDbPlatform
            strConnection = p_strConnection
            strAppServerName = p_strAppServerName
            strSchedulerFilePath = p_strSchedulerFilePath
            intScheduleId = p_intScheduleId
            strUserid = p_strUserid
            intRequestID = p_intRequestID
            intGroupId = p_intGroupId
            intJobRank = 1

            strJobOutputType = p_strJobOutputType
            strMsgTempCode = p_strMsgTempCode
            intIsImplicit = p_intIsImplicit
            intJobOrder = p_intJobOrder
            strServerIP = p_strServerIP
            dtRundate = p_Rundate

        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, ex.Message & ex.StackTrace, strUserid)
        End Try
    End Sub
    ' To Retrieve all the jobs in a group.
    Function GetJobs() As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim i As Integer
        Dim dsJobDetails As New DataSet
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If

                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select a.jobid,a.joborder,b.TYPEID,b.NAME,b.COMMAND,b.PROCESS,b.OUTPUT_FILE_NAME,b.OUTPUT_FILENAME_SP,b.PREPROCESS_SP,b.PREPROCESS_SP_PARAM_POS,c.NAME,b.ISSINGLETON "
                    strSql = strSql & " from sc_job_group_mapping a,sc_m_job b,sc_m_job_type c "
                    strSql = strSql & "where a.JOBID = b.JOBID and b.TYPEID=c.JOBTYPEID and groupid=" & intGroupId & " and a.JOBORDER =" & intJobOrder & " order by joborder"
                Else
                    strSql = "select a.jobid,a.joborder,b.TYPEID,b.NAME,b.COMMAND,b.PROCESS,b.OUTPUT_FILE_NAME,b.OUTPUT_FILENAME_SP,b.PREPROCESS_SP,b.PREPROCESS_SP_PARAM_POS,c.NAME,b.ISSINGLETON "
                    strSql = strSql & " from sc_job_group_mapping a,sc_m_job b,sc_m_job_type c "
                    strSql = strSql & "where a.JOBID = b.JOBID and b.TYPEID=c.JOBTYPEID and groupid=" & intGroupId & " and a.JOBORDER =" & intJobOrder & " order by joborder"
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBAdapter.SelectCommand = mDBCommand
                mDBAdapter.Fill(dsJobDetails)

                objStatus.bStatus = True
                objStatus.objReturn = dsJobDetails
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch ex As Exception
            objStatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            Return objStatus
        End Try
    End Function
    Function GetJobParameterValues(ByVal intJobId As Integer) As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim i As Integer
        Dim dsjobparams As New DataSet
        Dim strCommandToExe As String
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
                If objStatus.bStatus = False Then
                    Exit Function
                End If

                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select a.PARAMID,a.VALUE,b.NAME,b.PARAMTYPE ,a.PARAM_VAL_TYPE_ID,d.DATATYPE_NAME from SC_JOB_PARAMETER_VALUES a,sc_m_parameter b ,sc_m_job_parameters c, sc_datatype d"
                    strSql = strSql & " where a.PARAMID=b.PARAMID and a.JOBID=c.JOBID and a.PARAMID=c.PARMID and b.PARAMTYPE=d.DATATYPE_ID and a.JOBID =" & intJobId.ToString & " and a.SCHEDULEID=" & Me.intScheduleId.ToString & " order by param_order"
                Else
                    strSql = "select a.PARAMID,a.VALUE,b.NAME,b.PARAMTYPE ,a.PARAM_VAL_TYPE_ID,d.DATATYPE_NAME from SC_JOB_PARAMETER_VALUES a,sc_m_parameter b ,sc_m_job_parameters c, sc_datatype d"
                    strSql = strSql & " where a.PARAMID=b.PARAMID and a.JOBID=c.JOBID and a.PARAMID=c.PARMID and b.PARAMTYPE=d.DATATYPE_ID and a.JOBID =" & intJobId.ToString & " and a.SCHEDULEID=" & Me.intScheduleId.ToString & " order by param_order"
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBAdapter.SelectCommand = mDBCommand
                mDBAdapter.Fill(dsjobparams)

                objStatus.bStatus = True
                objStatus.objReturn = dsjobparams
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
            End If
        Catch ex As Exception

        End Try
    End Function

    Function GetReportArgs(ByVal Jobid As Integer, ByVal strJobname As String, ByVal strProcName As String, ByVal strProcParamPos As String, ByVal strProcess As String, ByVal strOutputFileName_SP As String, ByVal intHistID As Integer, ByRef strOutFilename As String, ByRef strLogFilename As String) As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim i As Integer
        Dim drJobs As IDataReader
        Dim dsArguments As New DataSet
        Dim strParameters As String
        Dim strArgs As String
        Dim strProcParams As String
        Dim strProcParamPositions() As String
        Dim strReportFileName As String
        Dim strReportLogFileName As String
        Dim j As Int16
        Dim strFileExtension As String
        Dim strExePath As String
        Dim ProcArgs As Object()
        Dim strParamvalue As String
        Dim strReportType As String


        Try
            strParameters = ""
            strArgs = ""
            strProcParamPositions = Nothing
            ProcArgs = Nothing
            If (strProcParamPos.Length) > 0 Then
                strProcParamPositions = strProcParamPos.Split(",")
            End If

            objStatus = GetJobParameterValues(Jobid)

            If objStatus.bStatus = False Then
                objStatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : GetReportArgs " & objStatus.strErrDescription, strUserid)
                Return objStatus
            End If

            'Retrieve the parameters 
            dsArguments = objStatus.objReturn

            strOutFilename = ""
            strLogFilename = ""

            If Not dsArguments Is Nothing Then
                For j = 0 To dsArguments.Tables(0).Rows.Count - 1

                    If Not dsArguments.Tables(0).Rows(j).Item("PARAM_VAL_TYPE_ID") Is System.DBNull.Value Then
                        If CInt(dsArguments.Tables(0).Rows(j).Item("PARAM_VAL_TYPE_ID")) = 2 Then
                            objStatus = GetProcOutput(dsArguments.Tables(0).Rows(j).Item("VALUE").ToString, strUserid)
                            If objStatus.bStatus = False Then
                                objStatus.bStatus = False
                                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : GetReportArgs " & objStatus.strErrDescription, strUserid)
                                Return objStatus
                            End If
                            If dsArguments.Tables(0).Rows(j).Item("DATATYPE_NAME").ToString.ToUpper.Equals("DATE") Then
                                strParamvalue = objStatus.objReturn
                                strParamvalue = Format(CDate(objStatus.objReturn), "dd/MM/yyyy")
                            Else
                                strParamvalue = objStatus.objReturn

                            End If
                            objStatus = insertDynamicParamValue(intRequestID, intScheduleId, Jobid, CInt(dsArguments.Tables(0).Rows(j).Item("PARAMID")), strParamvalue)
                            If objStatus.bStatus = False Then
                                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "ScheduleID: " & intScheduleId.ToString & " and Job Id : " & Jobid & objStatus.strErrDescription, strUserid)
                                abortTransaction()
                                UpdateStatusAsStop()
                                Exit Function
                            End If
                        ElseIf CInt(dsArguments.Tables(0).Rows(j).Item("PARAM_VAL_TYPE_ID")) = 3 Then
                            objStatus = CustomDateFunction(intScheduleId, Jobid, CInt(dsArguments.Tables(0).Rows(j).Item("PARAMID")), strUserid)
                            If objStatus.bStatus = False Then
                                objStatus.bStatus = False
                                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : GetReportArgs " & objStatus.strErrDescription, strUserid)
                                Return objStatus
                            End If
                            strParamvalue = objStatus.objReturn
                            objStatus = insertDynamicParamValue(intRequestID, intScheduleId, Jobid, CInt(dsArguments.Tables(0).Rows(j).Item("PARAMID")), strParamvalue)
                            If objStatus.bStatus = False Then
                                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "ScheduleID: " & intScheduleId.ToString & " and Job Id : " & Jobid & objStatus.strErrDescription, strUserid)
                                abortTransaction()
                                UpdateStatusAsStop()
                                Exit Function
                            End If
                        Else
                            strParamvalue = dsArguments.Tables(0).Rows(j).Item("value").ToString
                        End If
                    Else
                        objStatus.bStatus = False
                        MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : GetReportArgs " & " PARAM_VAL_TYPE_ID is DBNull ", strUserid)
                        Return objStatus
                    End If

                    If Left(UCase(dsArguments.Tables(0).Rows(j).Item("NAME")), 4) <> "PROC" Then
                        Select Case UCase(dsArguments.Tables(0).Rows(j).Item("DATATYPE_NAME"))
                            Case "VARCHAR", "DATE"
                                strParameters = strParameters & " """ & Trim(strParamvalue & "") & """"
                            Case "NUMBER", "NUMERIC"
                                strParameters = strParameters & " " & strParamvalue
                        End Select
                    End If
                Next

                If Not strProcParamPositions Is Nothing Then
                    ProcArgs = New Object(strProcParamPositions.Length - 1) {}
                    For j = 0 To strProcParamPositions.Length - 1
                        If IsNumeric(strProcParamPositions(j)) Then
                            Select Case dsArguments.Tables(0).Rows(CInt(strProcParamPositions(j)) - 1).Item("PARAMTYPE")
                                Case 1
                                    ProcArgs(j) = dsArguments.Tables(0).Rows(CInt(strProcParamPositions(j)) - 1)("VALUE")
                                    If ProcArgs(j) = "*" Then
                                        ProcArgs(j) = "%"
                                    End If
                                    strProcParams = strProcParams & " """ & Trim(ProcArgs(j) & "") & """"
                                Case 2
                                    ProcArgs(j) = dsArguments.Tables(0).Rows(CInt(strProcParamPositions(j)) - 1)("VALUE")
                                    If ProcArgs(j) = "*" Then
                                        ProcArgs(j) = "%"
                                    End If

                                    strProcParams = strProcParams & " " & ProcArgs(j)
                                Case 3
                                    ProcArgs(j) = dsArguments.Tables(0).Rows(CInt(strProcParamPositions(j)) - 1)("VALUE")
                                    If ProcArgs(j) = "*" Then
                                        ProcArgs(j) = "%"
                                    End If

                                    strProcParams = strProcParams & " " & ProcArgs(j)

                                Case 4
                                    ProcArgs(j) = dsArguments.Tables(0).Rows(CInt(strProcParamPositions(j)) - 1)("VALUE")

                                    strProcParams = strProcParams & " """ & Trim(ProcArgs(j) & "") & """"
                            End Select
                        End If
                    Next
                End If
            End If

            '******Added By Abdul To Get the Message Template Code
            '            strTemplateCode = strMsgTempCode

            Dim strArr(2) As String
            objStatus = GetReportOutputType(intScheduleId, intGroupId, Jobid)
            If objStatus.bStatus = False Then
                objStatus.bStatus = False
                Return objStatus
            End If

            strArr = objStatus.objReturn

            strFileExtension = strArr(0).Trim
            strReportType = strArr(1).Trim

            strArgs = strProcess & ""
            If Len(strParameters) > 0 Then
                strArgs = strArgs & " /P" & strParameters
            End If
            'Contructing Output File Name
            'Here we need to check if any custom procedure is Specify for 
            'this report 
            '****** Added By Abdul To Get The Report ID and Report Output File Name
            ' intReportID = Jobid
            If Not strOutputFileName_SP = Nothing Or Not strOutputFileName_SP = String.Empty Then
                objStatus = GetJoboutputFileName(Jobid, strOutputFileName_SP)
                If objStatus.bStatus = False Then
                    objStatus.bStatus = False
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : GetReportArgs " & objStatus.strErrDescription, strUserid)
                    Return objStatus
                End If
                strReportFileName = objStatus.objReturn
            Else
                strReportFileName = intHistID & "_" & strJobname
            End If

            objStatus = GetReportSourcePath()
            If objStatus.bStatus = False Then
                objStatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService :JobRequeests : GetReportArgs :" & objStatus.strErrDescription, strUserid)
                Return objStatus
            End If
            strExePath = objStatus.objReturn

            '******
            Dim strOutPath As String
            objStatus = Me.getOutputFolder(Jobid, "PORT")
            strOutPath = objStatus.objReturn

            If Not Directory.Exists(strOutPath) Then
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService :JobRequeests : GetReportArgs : " & strOutPath & " Folder is Does Not Exist", strUserid)
                Dim di As DirectoryInfo
                di = Directory.CreateDirectory(strOutPath)
            End If
            
            strReportLogFileName = strReportFileName & ".log"
            strReportFileName = strReportFileName & strFileExtension '".pdf"
            strArgs = strArgs & " /S " & strExePath & "\crystal"
            strArgs = strArgs & " /O " & strReportFileName
            strArgs = strArgs & " /D " & strOutPath
            strArgs = strArgs & " /L " & strReportLogFileName
            strArgs = strArgs & " /E " & strOutPath
            strArgs = strArgs & " /F " & strReportType
            If strProcName.Length > 0 Then
                strArgs = strArgs & " /U " & strProcName
            End If
            If Not strProcParams = Nothing Then
                If strProcParams.Length > 0 Then
                    strArgs = strArgs & " /V" & strProcParams
                End If
            End If

            strOutFilename = strReportFileName
            strLogFilename = strReportLogFileName

            objStatus.bStatus = True
            objStatus.objReturn = strArgs
            Return objStatus
        Catch ex As Exception
            objStatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService :JobRequeests : GetReportArgs :" & ex.Message, strUserid)
            Return objStatus
        End Try
    End Function
    Private Function GetReportOutputType(ByVal intScheduleid As Integer, ByVal intGroupid As Integer, ByVal intJobid As Integer) As MQSCommonObj.MQSStatus
        Dim objstatus As New MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim strSql1 As String
        Dim objTypeExt As Object
        Dim objJobtype As Object
        Dim RetVal(2) As String
        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objstatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objstatus = openConnection()
                End If
                If objstatus.bStatus = False Then
                    Exit Function
                End If


                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select b.EXTENSION from SC_SCHD_GRP_JOBS_DETAILS a,sc_report_types b where upper(a.JOB_OUTPUT_TYPE)=upper(b.NAME) and SCHEDULEID=" & intScheduleid & " and GROUPID=" & intGroupid & " and JOBID=" & intJobid
                Else
                    strSql = "select b.EXTENSION from SC_SCHD_GRP_JOBS_DETAILS a,sc_report_types b where upper(a.JOB_OUTPUT_TYPE)=upper(b.NAME) and SCHEDULEID=" & intScheduleid & " and GROUPID=" & intGroupid & " and JOBID=" & intJobid
                End If

                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql1 = "select a.JOB_OUTPUT_TYPE from SC_SCHD_GRP_JOBS_DETAILS a,sc_report_types b where upper(a.JOB_OUTPUT_TYPE)=upper(b.NAME) and SCHEDULEID=" & intScheduleid & " and GROUPID=" & intGroupid & " and JOBID=" & intJobid
                Else
                    strSql1 = "select a.JOB_OUTPUT_TYPE from SC_SCHD_GRP_JOBS_DETAILS a,sc_report_types b where upper(a.JOB_OUTPUT_TYPE)=upper(b.NAME) and SCHEDULEID=" & intScheduleid & " and GROUPID=" & intGroupid & " and JOBID=" & intJobid
                End If
            End If
            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            objTypeExt = mDBCommand.ExecuteScalar()

            mDBCommand.CommandText = strSql1
            mDBCommand.CommandType = CommandType.Text
            objJobtype = mDBCommand.ExecuteScalar()

            RetVal(0) = CStr(objTypeExt)
            RetVal(1) = CStr(objJobtype)

            objstatus.bStatus = True
            objstatus.objReturn = RetVal
            Return objstatus
        Catch ex As Exception
            objstatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService :JobRequeests : GetReportSourcePath : " & ex.Message, strUserid)
            Return objstatus
        End Try
    End Function
    Private Function GetReportSourcePath() As MQSCommonObj.MQSStatus
        Dim objstatus As New MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim objPath As Object

        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objstatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objstatus = openConnection()
                End If
                If objstatus.bStatus = False Then
                    Exit Function
                End If


                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "select option_value from application_profile where upper(option_name)='REPORTS_EXE_DIR'"
                Else
                    strSql = "select option_value from application_profile where upper(option_name)='REPORTS_EXE_DIR'"
                End If
            End If
            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            objPath = mDBCommand.ExecuteScalar()

            If objPath Is Nothing Or objPath.ToString = String.Empty Then
                objstatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Report Exe Path is not specify in application Profile", strUserid)
                Return objstatus
            End If

            objstatus.bStatus = True
            objstatus.objReturn = objPath.ToString
            Return objstatus
        Catch ex As Exception
            objstatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService :JobRequeests : GetReportSourcePath : " & ex.Message, strUserid)
            Return objstatus
        End Try
    End Function
    Private Function GetJoboutputFileName(ByVal p_Jobid As Integer, ByVal strFilename_SP As String) As MQSCommonObj.MQSStatus

        Dim strsql As String
        Dim objProc_Name As Object
        Dim outFilename As String
        Dim objstatus As MQSCommonObj.MQSStatus
        outFilename = String.Empty
        Dim tmpOutParam As IDbDataParameter
        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objstatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objstatus = openConnection()
                End If
                If objstatus.bStatus = False Then
                    Exit Function
                End If

                mDBCommand.CommandText = strFilename_SP
                mDBCommand.CommandType = CommandType.StoredProcedure

                If strDbPlatform.ToUpper = "ORACLE" Then
                    tmpOutParam = New OracleParameter
                    tmpOutParam.ParameterName = "RPT_NAME"
                    tmpOutParam.Direction = ParameterDirection.Output
                    tmpOutParam.DbType = DbType.String
                    tmpOutParam.Size = 50
                Else
                    tmpOutParam = New SqlParameter
                    tmpOutParam.ParameterName = "@" + "RPT_NAME"
                    tmpOutParam.Direction = ParameterDirection.Output
                    tmpOutParam.DbType = DbType.String
                    tmpOutParam.Size = 50
                End If
            End If

            mDBCommand.Parameters.Add(tmpOutParam)
            mDBCommand.ExecuteNonQuery()

            If strDbPlatform.ToUpper = "ORACLE" Then
                tmpOutParam = mDBCommand.Parameters.Item("RPT_NAME")
            Else
                tmpOutParam = mDBCommand.Parameters.Item("@RPT_NAME")
            End If

            outFilename = tmpOutParam.Value

            objstatus.bStatus = True
            objstatus.objReturn = outFilename
            Return objstatus
        Catch ex As Exception
            objstatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest  : GetReportFileName : " & ex.Message & ex.StackTrace, " ")
            Return objstatus
        Finally
            'Return outFilename
        End Try
    End Function
    'Method Name : nullTrim
    'Description : Trim the if any null character is present in string.
    '              
    'Author      : Abdul
    Private Function NullTrim(ByVal s As String) As String
        Dim n As Integer
        Try
            n = InStr(s, Chr(0))
            If (n > 0) Then
                NullTrim = Trim(Left(s, n - 1))
            Else
                NullTrim = Trim(s)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : nullTrim : " & ex.Message & ex.StackTrace, " ")
        End Try
    End Function
    Private Function UpdateReportStatus(ByVal strCode As String, ByVal intHistoryId As Integer)
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim strOutcome As String
        Dim strPerOfComp As String
        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If

                If strCode.Trim.ToUpper = "S" Then
                    strOutcome = "Completed Successfully"
                Else
                    strOutcome = "Completed With Error"
                End If
                strPerOfComp = "100"
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "Update SC_JOB_HISTORY set outcome='" & strOutcome & "',PRCNT_REC_PROCESSED='" & strPerOfComp & "' where Hist_id=" & intHistoryId
                Else
                    strSql = "Update SC_JOB_HISTORY set outcome='" & strOutcome & "',PRCNT_REC_PROCESSED='" & strPerOfComp & "' where Hist_id=" & intHistoryId
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBCommand.ExecuteNonQuery()

                objStatus.bStatus = True
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & " Error While try to Update the History Outcome", strUserid)
            End If

        Catch ex As Exception
            objStatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UpdateReportStatus : ScheduleID:" & intScheduleId & ex.Message & ex.StackTrace, strUserid)
            Return objStatus
        End Try
    End Function
    Private Function UpdateExpGroupRequest()
        Dim objstatus As New MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim intJobOrderCount As Integer

        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objstatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objstatus = openConnection()
                End If
            Else
                objstatus.bStatus = False
                Return objstatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & " Connection is Nothing", strUserid)
            End If

            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "SELECT COUNT(*) FROM SC_JOB_GROUP_MAPPING WHERE GROUPID =" & intGroupId & " AND JOBORDER > " & intJobOrder
            Else
                strSql = "SELECT COUNT(*) FROM SC_JOB_GROUP_MAPPING WHERE GROUPID =" & intGroupId & " AND JOBORDER > " & intJobOrder
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            intJobOrderCount = mDBCommand.ExecuteScalar()

            If intJobOrderCount > 0 Then
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "UPDATE SC_SCHEDULE_REQUEST SET STATUS='N', NEXTJOBORDER =" & intJobOrder + 1 & " WHERE  SCREQUESTID =" & intRequestID
                Else
                    strSql = "UPDATE SC_SCHEDULE_REQUEST SET STATUS='N', NEXTJOBORDER =" & intJobOrder + 1 & " WHERE  SCREQUESTID =" & intRequestID
                End If
            Else
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "UPDATE SC_SCHEDULE_REQUEST SET STATUS='CO', NEXTJOBORDER =0"
                    strSql = strSql & " , RUN_DURATION=(SELECT SUM(RUN_DURATION) FROM SC_JOB_HISTORY a WHERE a.SCREQUESTID=" & intRequestID & ")"
                    strSql = strSql & " WHERE  SCREQUESTID =" & intRequestID
                Else
                    strSql = "UPDATE SC_SCHEDULE_REQUEST SET STATUS='CO', NEXTJOBORDER =0"
                    strSql = strSql & " , RUN_DURATION=(SELECT SUM(RUN_DURATION) FROM SC_JOB_HISTORY a WHERE a.SCREQUESTID=" & intRequestID & ")"
                    strSql = strSql & " WHERE  SCREQUESTID =" & intRequestID
                End If
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            mDBCommand.ExecuteNonQuery()

            objstatus.bStatus = True
            Return objstatus
        Catch ex As Exception
            objstatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UpdateReportStatus : ScheduleID:" & intScheduleId & ex.Message & ex.StackTrace, strUserid)
            Return objstatus
        End Try
    End Function
    Private Function SendJobOutputByEmail(ByVal intScId As Integer, ByVal intRequestID As Integer, ByVal iHistoryId As Integer, ByVal intGroupId As Integer)
        'Dim objDBQuery As New DBQuery
        Dim strsql As String
        Dim strCurrent_User As String
        Dim intRequest_Id As Integer

        Dim objdt As DataTable
        Dim objToEmail As Object
        Dim objFromEmail As Object
        Dim objFileOutputDir As Object
        Dim strTemplate_code As String
        Dim strAttachment(100) As String
        Dim objst As Object
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim i As Int16
        Dim objMail As MQSSendMails.MQSubscribe.SendMails
        Dim dsUsers As DataSet
        Dim strOutPutfolder As String
        Dim strOutputfile As String
        Dim intJobCount As Object

        Try
            'Retrieve the Output folder for Schedule.
            strsql = "Select count(*) from SC_SCHD_GRP_JOBS_DETAILS where upper(sendemail)='N' and scheduleid=" & intScheduleId.ToString
            intJobCount = MQSDB.MQSDataprovider.mqsExecuteScalar(strsql, MQSDB.MQSDataprovider.MQSCommandType.Text, strUserid)

            If intJobCount Is Nothing OrElse CInt(intJobCount) = 0 Then
                objStatus.bStatus = True
                objStatus.strErrDescription = "No Job is Specified For Send output by Email"
                Return objStatus
            End If

            Dim dsHistIds As New DataSet
            Dim dsOutPut As New DataSet

            'Retrive all the History IDs for that current Request.

            objStatus = GetHistoryIds(intRequestID)
            If objStatus.bStatus = False Then
                objStatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail " & objStatus.strErrDescription, strUserid)
                Return objStatus
            End If

            dsHistIds = objStatus.objReturn

            'Checking Whether mail will be send for Implicit Group or Explicit Group.
            If intIsImplicit = 0 Then

                For i = 0 To dsHistIds.Tables(0).Rows.Count - 1

                    objStatus = GetJoboutputFile(dsHistIds.Tables(0).Rows(i).Item("Hist_id"))
                    If objStatus.bStatus = False Then
                        objStatus.bStatus = False
                        MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail " & objStatus.strErrDescription, strUserid)
                        Return objStatus
                    End If
                    strOutputfile = objStatus.objReturn

                    objStatus = getOutputFolder(dsHistIds.Tables(0).Rows(i).Item("jobid"), strUserid)

                    If objStatus.bStatus = False Then
                        objStatus.bStatus = False
                        MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail " & objStatus.strErrDescription, strUserid)
                        Return objStatus
                    End If

                    strOutPutfolder = objStatus.objReturn
                    If Not File.Exists(strOutPutfolder & "\" & dsHistIds.Tables(0).Rows(i).Item("name").ToString & "\" & strOutputfile) Then
                        objStatus.bStatus = False
                        MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail: Output File Does Not Exist. History Id:" & iHistoryId, strUserid)
                        Return objStatus
                    End If

                    strAttachment(i) = strOutPutfolder & "\" & dsHistIds.Tables(0).Rows(i).Item("name").ToString & "\" & strOutputfile
                Next
            Else
                objStatus = GetJoboutputFile(dsHistIds.Tables(0).Rows(i).Item("Hist_id"))
                If objStatus.bStatus = False Then
                    objStatus.bStatus = False
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail " & objStatus.strErrDescription, strUserid)
                    Return objStatus
                End If
                strOutputfile = objStatus.objReturn

                objStatus = getOutputFolder(dsHistIds.Tables(0).Rows(i).Item("jobid"), strUserid)

                If objStatus.bStatus = False Then
                    objStatus.bStatus = False
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail " & objStatus.strErrDescription, strUserid)
                    Return objStatus
                End If

                strOutPutfolder = objStatus.objReturn

                If Not File.Exists(strOutPutfolder & "\" & dsHistIds.Tables(0).Rows(i).Item("name").ToString & "\" & strOutputfile) Then
                    objStatus.bStatus = False
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail: Output File Does Not Exist. History Id:" & iHistoryId, strUserid)
                    Return objStatus
                End If
                ' If Not dsUsers Is Nothing AndAlso dsUsers.Tables(0).Rows.Count > 0 Then
                strAttachment(0) = strOutPutfolder & "\" & dsHistIds.Tables(0).Rows(i).Item("name").ToString & "\" & strOutputfile
            End If
            objStatus = GetFromEmailid(intScId)

            If objStatus.bStatus = False Then
                objStatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail " & objStatus.strErrDescription, strUserid)
                Return objStatus
            End If
            objFromEmail = objStatus.objReturn


            objStatus = GetUserEmailids(intScId, strUserid)
            If objStatus.bStatus = False Then
                objStatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail " & objStatus.strErrDescription, strUserid)
                Return objStatus
            End If
            dsUsers = objStatus.objReturn

            If dsUsers Is Nothing Then
                objStatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail : dsUsers is Nothing", strUserid)
                Return objStatus
            End If

            If dsUsers.Tables(0).Rows.Count = 0 Then
                objStatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail : No User Register To Get Mail", strUserid)
                Return objStatus
            End If

            objStatus = GetTemplateCode(intScId)
            If objStatus.bStatus = False Then
                objStatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail " & objStatus.strErrDescription, strUserid)
                Return objStatus
            End If
            strTemplate_code = objStatus.objReturn

            Dim strUsers As String

            For i = 0 To dsUsers.Tables(0).Rows.Count - 1
                strUsers = strUsers & dsUsers.Tables(0).Rows(i).Item("Email").ToString & ";"
            Next

            objMail = New MQSSendMails.MQSubscribe.SendMails
            objStatus = objMail.MailTemplate(objFromEmail.ToString, strUsers, strTemplate_code, intRequestID, strAttachment)
            If objStatus.bStatus = False Then
                objStatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail " & objStatus.strErrDescription, strUserid)
                objStatus = UpdateSendmailStatus("F", iHistoryId)

                If objStatus.bStatus = False Then
                    objStatus.bStatus = False
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail " & objStatus.strErrDescription, strUserid)
                    Return objStatus
                End If
                Return objStatus
            End If
            objStatus = UpdateSendmailStatus("S", iHistoryId)

            If objStatus.bStatus = False Then
                objStatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail " & objStatus.strErrDescription, strUserid)
                Return objStatus
            End If

            objStatus.bStatus = True
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "JobRequest : SendJobOutputByEmail : " & ex.Message, " ")
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message.ToString
            Return objStatus
        End Try
    End Function
    Function GetHistoryIds(ByVal intRequestID As Integer) As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim dsHistIds As New DataSet
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & " Connection is Nothing", strUserid)
            End If

            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "Select Hist_id,C.NAME,c.JOBID from Sc_job_history a,SC_SCHD_GRP_JOBS_DETAILS B,SC_M_JOB C where A.JOB_ID = B.JOBID And A.SCHEDULE_ID = B.SCHEDULEID AND A.JOB_ID=C.JOBID And upper(b.SENDEMAIL)='N' and a.SCREQUESTID = " & intRequestID
            Else
                strSql = "Select Hist_id,C.NAME,c.JOBID from Sc_job_history a,SC_SCHD_GRP_JOBS_DETAILS B,SC_M_JOB C where A.JOB_ID = B.JOBID And A.SCHEDULE_ID = B.SCHEDULEID AND A.JOB_ID=C.JOBID And upper(b.SENDEMAIL)='N' And A.SCREQUESTID= " & intRequestID
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            mDBAdapter.SelectCommand = mDBCommand
            mDBAdapter.Fill(dsHistIds)

            objStatus.bStatus = True
            objStatus.objReturn = dsHistIds
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, ex.Message, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            objStatus = Nothing
        End Try
    End Function
    Function UpdateSendmailStatus(ByVal strStatus As String, ByVal iHistoryId As Integer) As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & " Connection is Nothing", strUserid)
            End If

            If strDbPlatform.ToUpper = "ORACLE" Then
                If strStatus.Trim.ToUpper.Equals("S") Then
                    strSql = "Update sc_job_history set EMAIL_SEND_STATUS='S' where hist_id=" & iHistoryId
                ElseIf strStatus.Trim.ToUpper.Equals("F") Then
                    strSql = "Update sc_job_history set EMAIL_SEND_STATUS='F' where hist_id=" & iHistoryId
                ElseIf strStatus.Trim.ToUpper.Equals("N") Then
                    strSql = "Update sc_job_history set EMAIL_SEND_STATUS='N' where hist_id=" & iHistoryId
                ElseIf strStatus.Trim.ToUpper.Equals("NA") Then
                    strSql = "Update sc_job_history set EMAIL_SEND_STATUS='NA' where hist_id=" & iHistoryId
                End If
            Else
                If strStatus.Trim.ToUpper.Equals("S") Then
                    strSql = "Update sc_job_history set EMAIL_SEND_STATUS='S' where hist_id=" & iHistoryId
                ElseIf strStatus.Trim.ToUpper.Equals("F") Then
                    strSql = "Update sc_job_history set EMAIL_SEND_STATUS='F' where hist_id=" & iHistoryId
                ElseIf strStatus.Trim.ToUpper.Equals("N") Then
                    strSql = "Update sc_job_history set EMAIL_SEND_STATUS='N' where hist_id=" & iHistoryId
                ElseIf strStatus.Trim.ToUpper.Equals("NA") Then
                    strSql = "Update sc_job_history set EMAIL_SEND_STATUS='NA' where hist_id=" & iHistoryId
                End If
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            mDBCommand.ExecuteNonQuery()

            objStatus.bStatus = True
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, ex.Message, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            objStatus = Nothing
        End Try

    End Function
    Function GetFromEmailid(ByVal intScId As Integer)
        Dim strSql As String
        Dim objCreatedBy As Object
        Dim objEmailid As Object
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & " Connection is Nothing", strUserid)
            End If

            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "select Created_By from sc_job_Schedule_info where Scheduleid=" & intScId
            Else
                strSql = "select Created_By from sc_job_Schedule_info where Scheduleid=" & intScId
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            objCreatedBy = mDBCommand.ExecuteScalar()

            If objCreatedBy Is System.DBNull.Value Then
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Output File is not Present For Schedule Id :" & intScId, strUserid)
                objStatus.bStatus = False
                Return objStatus
            End If


            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "Select Email from am_users where upper(user_id)='" & objCreatedBy.ToString.ToUpper & "'"
            Else
                strSql = "Select Email from am_users where upper(user_id)='" & objCreatedBy.ToString.ToUpper & "'"
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            objEmailid = mDBCommand.ExecuteScalar()

            If objEmailid Is System.DBNull.Value Then
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "Select Option_value from application_profile where upper(option_name)='DEFAULTEMAIL_ADDRESS'"
                Else
                    strSql = "Select Option_value from application_profile where upper(option_name)='DEFAULTEMAIL_ADDRESS'"
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                objEmailid = mDBCommand.ExecuteScalar()


                If objEmailid Is System.DBNull.Value Then
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : GetFromEmailid : No Default Email Address Specify in Application_Profile ", strUserid)
                    objStatus.bStatus = False
                    Return objStatus
                End If
            End If

            objStatus.bStatus = True
            objStatus.objReturn = objEmailid.ToString
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, ex.Message, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            objStatus = Nothing
        End Try
    End Function

    Function GetTemplateCode(ByVal intScId As Integer)
        Dim strSql As String
        Dim objTemplateCode As Object
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & " Connection is Nothing", strUserid)
            End If

            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "select JOB_OUTPUT_MSG_TEMP_CODE from sc_job_Schedule_info where Scheduleid=" & intScId
            Else
                strSql = "select JOB_OUTPUT_MSG_TEMP_CODE from sc_job_Schedule_info where Scheduleid=" & intScId
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text

            objTemplateCode = mDBCommand.ExecuteScalar()

            If objTemplateCode Is System.DBNull.Value Then
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Output File is not Present For Schedule Id :" & intScId, strUserid)
                objStatus.bStatus = False
                Return objStatus
            End If

            objStatus.bStatus = True
            objStatus.objReturn = objTemplateCode.ToString
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, ex.Message, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            objStatus = Nothing
        End Try
    End Function
    Function GetJoboutputFile(ByVal iHistory_id As Integer)
        Dim strSql As String
        Dim objJoboutputFileName As Object
        Dim dsOutput As New DataSet
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & " Connection is Nothing", strUserid)
            End If

            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "select A.OUTPUT_FILENAME from sc_job_History A,SC_SCHD_GRP_JOBS_DETAILS b " & _
                " where a.JOB_ID=b.JOBID and a.SCHEDULE_ID=b.SCHEDULEID and upper(b.SENDEMAIL)='N' and Hist_id=" & iHistory_id
            Else
                strSql = "select A.OUTPUT_FILENAME from sc_job_History A,SC_SCHD_GRP_JOBS_DETAILS b " & _
                " where a.JOB_ID=b.JOBID and a.SCHEDULE_ID=b.SCHEDULEID and upper(b.SENDEMAIL)='N' and Hist_id=" & iHistory_id
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            objJoboutputFileName = mDBCommand.ExecuteScalar()
            objStatus.bStatus = True
            objStatus.objReturn = objJoboutputFileName.ToString
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, ex.Message, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            objStatus = Nothing
        End Try
    End Function
    Function GetScheduleType(ByVal iSchId As Integer, ByVal strUserId As String) As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim dsScJobType As New DataSet
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & " Connection is Nothing", strUserId)
            End If

            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "select d.name from SC_Job_Schedule_Info a ,sc_job_group_mapping b ,sc_m_job c ,sc_m_job_type d "
                strSql = strSql & " where a.groupid=b.groupid and b.jobid=c.jobid and c.typeid = d.jobtypeid And a.scheduleid = " & iSchId
            Else
                strSql = "select d.name from SC_Job_Schedule_Info a ,sc_job_group_mapping b ,sc_m_job c ,sc_m_job_type d "
                strSql = strSql & " where a.groupid=b.groupid and b.jobid=c.jobid and c.typeid = d.jobtypeid And a.scheduleid = " & iSchId
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            mDBAdapter.SelectCommand = mDBCommand
            mDBAdapter.Fill(dsScJobType)

            objStatus.bStatus = True
            objStatus.objReturn = dsScJobType
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, ex.Message, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            objStatus = Nothing
        End Try
    End Function

    Function getOutputFolder(ByVal intJobid As Integer, ByVal strUserId As String) As MQSCommonObj.MQSStatus
        Dim strSql As String
        Dim dsScJobType As New DataSet
        Dim strOption_name As String
        Dim strFolder_Name As String
        Dim strJobType As String
        Dim objJobtype As Object
        Dim strJobName As String
        Dim objJobTypeOutDir As Object
        Dim obj1 As Object
        Dim objJobName As Object
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try

            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objStatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objStatus = openConnection()
                End If
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : getOutputFolder: ScheduleID:" & intScheduleId & objStatus.strErrDescription, strUserId)
            End If
            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "Select b.OUTPUT_DIR from sc_m_job a,sc_m_job_type b where a.TYPEID=b.JOBTYPEID and a.jobid=" & intJobid
            Else
                strSql = "Select b.OUTPUT_DIR from sc_m_job a,sc_m_job_type b where a.TYPEID=b.JOBTYPEID and a.jobid=" & intJobid
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            objJobTypeOutDir = mDBCommand.ExecuteScalar()

            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "Select b.name from sc_m_job a,sc_m_job_type b where a.TYPEID=b.JOBTYPEID and a.jobid=" & intJobid
            Else
                strSql = "Select b.name from sc_m_job a,sc_m_job_type b where a.TYPEID=b.JOBTYPEID and a.jobid=" & intJobid
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            objJobtype = mDBCommand.ExecuteScalar()

            If objJobtype Is Nothing OrElse objJobtype Is System.DBNull.Value Then
                objStatus.bStatus = False
                objStatus.strErrDescription = "Job Type is Nothing "
                Return objStatus
            End If
            strJobType = objJobtype.ToString

            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "Select upper(NAME) from sc_m_job where jobid=" & intJobid
            Else
                strSql = "Select upper(NAME) from sc_m_job where jobid=" & intJobid
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            objJobName = mDBCommand.ExecuteScalar()

            If objJobName Is Nothing OrElse objJobName Is System.DBNull.Value Then
                objStatus.bStatus = False
                objStatus.strErrDescription = "Job Name is Empty"
                Return objStatus
            End If

            strJobName = objJobName.ToString

            If objJobTypeOutDir Is System.DBNull.Value Then
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "SELECT OPTION_VALUE FROM APPLICATION_PROFILE WHERE upper(OPTION_NAME)='SCHEDULERFILEPATH'"
                Else
                    strSql = "SELECT OPTION_VALUE FROM APPLICATION_PROFILE WHERE upper(OPTION_NAME)='SCHEDULERFILEPATH'"
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                strFolder_Name = mDBCommand.ExecuteScalar()

                If strJobType.Trim.ToUpper = "REPORTS" Then
                    strFolder_Name = strFolder_Name & "\REPORTS\" & strJobName
                Else
                    strFolder_Name = strFolder_Name & "\" & strJobName
                End If
            Else
                strFolder_Name = objJobTypeOutDir.ToString & "\" & strJobName
            End If

            objStatus.bStatus = True
            objStatus.objReturn = strFolder_Name
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : getOutputFolder" & ex.Message, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            objStatus = Nothing
        End Try
    End Function
    Function GetUserEmailids(ByVal intScId As Integer, ByVal strUserid As String) As MQSCommonObj.MQSStatus
        Dim objstatus As New MQSCommonObj.MQSStatus
        Dim dsUsers As New DataSet
        Dim strSql As String

        Try
            If Not strDbPlatform Is Nothing Then
                If mDBConnection Is Nothing Then
                    objstatus = openConnection()
                ElseIf mDBConnection.State = ConnectionState.Closed Then
                    objstatus = openConnection()
                End If
            Else
                objstatus.bStatus = False
                Return objstatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest : UPdateReportStatus: ScheduleID:" & intScheduleId & " Connection is Nothing", strUserid)
            End If

            If strDbPlatform.ToUpper = "ORACLE" Then
                strSql = "SELECT B.USER_ID,B.EMAIL FROM SC_JOB_OUTPUT_RECIPIENTS A,AM_USERS B WHERE upper(A.USER_ID) = upper(B.USER_ID) and upper(a.STATUS)='A' And B.EMAIL Is Not NULL And A.SCHEDULE_ID =" & intScId
            Else
                strSql = "SELECT B.USER_ID,B.EMAIL FROM SC_JOB_OUTPUT_RECIPIENTS A,AM_USERS B WHERE upper(A.USER_ID) = upper(B.USER_ID) and upper(a.STATUS)='A' And B.EMAIL Is Not NULL And A.SCHEDULE_ID =" & intScId
            End If

            mDBCommand.CommandText = strSql
            mDBCommand.CommandType = CommandType.Text
            mDBAdapter.SelectCommand = mDBCommand
            mDBAdapter.Fill(dsUsers)

            objstatus.bStatus = True
            objstatus.objReturn = dsUsers
            Return objstatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService : JobRequest :GetUserEmailids" & ex.Message, strUserid)
            objstatus.bStatus = False
            objstatus.strErrDescription = ex.Message
            Return objstatus
        Finally
            objstatus = Nothing
        End Try
    End Function
End Class
