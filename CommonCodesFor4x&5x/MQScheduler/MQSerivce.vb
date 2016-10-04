Imports System.Timers
Imports System.Data
Imports Oracle.DataAccess.Client
Imports System.Data.SqlClient
Imports System.Runtime.InteropServices
Imports System.Web.Mail
Imports System.IO
Imports System.Threading
Imports System.ServiceProcess
Public Enum weekly
    Sunday = 1
    Monday = 2
    Tuesday = 4
    Wednesday = 8
    Thursday = 16
    Friday = 32
    Saturday = 64
End Enum
Public Enum monthly
    January = 1
    February = 2
    March = 4
    April = 8
    May = 16
    June = 32
    July = 64
    August = 128
    September = 256
    October = 512
    November = 1024
    December = 2048
End Enum
Public Enum MonthlyRelative
    Sunday = 1
    Monday = 2
    Tuesday = 3
    Wednesday = 4
    Thursday = 5
    Friday = 6
    Saturday = 7
    Day = 8
    WeekDay = 9
    WeekenDay = 10
End Enum
Public Enum freq_relative_interval
    First = 1
    Second = 2
    Third = 4
    Fourth = 8
    Last = 16
End Enum
Public Enum freq_type
    Once = 1
    Daily = 4
    Weekly = 8
    Monthly = 16
    Monthly_relative_freq = 32
End Enum
Public Class MQScheduler
    Inherits System.ServiceProcess.ServiceBase
    Public intProcessCount As Integer = 0
    Dim strDbPlatform As String
    Dim strConnection As String
    Dim strAppServerName As String
    Dim strSchedulerFilePath As String
    Dim MQTimer As System.Timers.Timer
    Dim strOledbConnection As String
    Dim mDBTransaction As IDbTransaction
    Dim mDBCommand As IDbCommand
    Dim mDBConnection As IDbConnection
    Dim mDBAdapter As IDbDataAdapter
    Dim strServerIP As String
    Dim t As Thread
    Dim objJob As JobRequest
#Region " Component Designer generated code "

    Public Sub New()
        MyBase.New()

        ' This call is required by the Component Designer.
        InitializeComponent()

        ' Add any initialization after the InitializeComponent() call

    End Sub

    'UserService overrides dispose to clean up the component list.
    Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
        If disposing Then
            If Not (components Is Nothing) Then
                components.Dispose()
            End If
        End If
        MyBase.Dispose(disposing)
    End Sub

    ' The main entry point for the process
    <MTAThread()> _
    Shared Sub Main()
        Dim ServicesToRun() As System.ServiceProcess.ServiceBase

        ' More than one NT Service may run within the same process. To add
        ' another service to this process, change the following line to
        ' create a second service object. For example,
        '
        '   ServicesToRun = New System.ServiceProcess.ServiceBase () {New Service1, New MySecondUserService}
        '
        ServicesToRun = New System.ServiceProcess.ServiceBase() {New MQScheduler}

        System.ServiceProcess.ServiceBase.Run(ServicesToRun)
    End Sub

    'Required by the Component Designer
    Private components As System.ComponentModel.IContainer

    ' NOTE: The following procedure is required by the Component Designer
    ' It can be modified using the Component Designer.  
    ' Do not modify it using the code editor.
    <System.Diagnostics.DebuggerStepThrough()> Private Sub InitializeComponent()
        '
        'MQScheduler
        '
        Me.ServiceName = "MQScheduler"

    End Sub

#End Region

    Protected Overrides Sub OnStart(ByVal args() As String)
        Dim objStatus As MQSCommonObj.MQSStatus
        Dim strUserid As String = "PORT"
        Dim blnStatus As Boolean
        Try
            'event will fire before starting the service 
            Dim ci As New System.Globalization.CultureInfo(System.Threading.Thread.CurrentThread.CurrentCulture.LCID)
            ci.DateTimeFormat.ShortDatePattern = "dd/MM/yyyy"
            ci.DateTimeFormat.DateSeparator = "/"
            System.Threading.Thread.CurrentThread.CurrentCulture = ci
            ci.DateTimeFormat.LongTimePattern = "hh:mm:ss tt"

            'write to log that service has started
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "On Start:Process Started", strUserid)

            'get the ini parametes
            objStatus = getINIParamValues()
            If objStatus.bStatus = False Then
                'stop the service if don't get the ini value
                StopService()
                Exit Sub
            End If
            If objStatus.bStatus = True Then
                'set the server name
                If strDbPlatform = "" Or strConnection = "" Then
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection String Not Found", strUserid)
                    StopService()
                    Exit Sub
                End If
                strAppServerName = System.Environment.MachineName
                'update the Schedule Requests
                objStatus = UpdateScheduleRequests("PORT")
                If objStatus.bStatus = True Then
                    CommitTransaction()
                Else
                    abortTransaction()
                End If
                'get the logs file path
                objStatus = getSchedulerFilePath(strUserid)
                If objStatus.bStatus = False Then
                    abortTransaction()
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, objStatus.strErrDescription, "PORT")
                    Exit Sub
                Else
                    CommitTransaction()
                    strSchedulerFilePath = objStatus.objReturn
                End If

                'Timer which start the process

                MQTimer = New System.Timers.Timer
                AddHandler MQTimer.Elapsed, AddressOf OnTimerElapse

                'set the timer intervel 
                MQTimer.Interval = 100
                MQTimer.Enabled = True

            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "On Start:" & ex.Message & ex.StackTrace, strUserid)
        End Try
    End Sub
    Protected Overrides Sub OnStop()
        Dim objStatus As MQSCommonObj.MQSStatus
        Dim strUserid As String = "PORT"
        Try
            'event will fire before stoping the service
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "On Stop", strUserid)

            'update the Schedule Requests
            objStatus = UpdateScheduleRequests("PORT")

            If objStatus.bStatus = True Then
                CommitTransaction()
            Else
                abortTransaction()
            End If

        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, ex.Message, "PORT")
        End Try
    End Sub
    Function getSchedulerFilePath(ByVal strUserId As String) As MQSCommonObj.MQSStatus
        'To get the Scheduler File path
        Dim strSql As String
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim i As Integer
        Dim objFilePath As Object
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
                    strSql = "select option_value from application_profile where upper(option_name) ='SCHEDULERFILEPATH'"
                Else
                    strSql = "select option_value from application_profile with (NOLOCK) where upper(option_name) ='SCHEDULERFILEPATH'"
                End If
                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                objFilePath = mDBCommand.ExecuteScalar()

                If objFilePath Is System.DBNull.Value OrElse CStr(objFilePath) = String.Empty Then
                    objStatus.bStatus = False
                    objStatus.strErrDescription = "Schedule File Path is Not Specified In Application Profile"
                    Return objStatus
                End If
                objStatus.bStatus = True
                objStatus.objReturn = CStr(objFilePath)
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserId)
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "getSchedulerFilePath :" & ex.Message & ex.StackTrace, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            getSchedulerFilePath = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function RetrievePendingRequests() As MQSCommonObj.MQSStatus
        'to get the pending Requests
        Dim strSql As String
        Dim strCurDt As String
        Dim strUpdateCommand As String
        Dim dsRequests As New DataSet
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim con As IDbConnection
        Dim cmd As IDbCommand
        Dim strUserid As String = "PORT"
        Dim objAdapt As Object
        Try
            If Not strDbPlatform Is Nothing Then
                If strDbPlatform.ToUpper = "ORACLE" Then
                    con = New OracleConnection(strConnection)
                    objAdapt = New OracleDataAdapter
                    strSql = "select a.SCREQUESTID, a.SCHEDULEID, a.GROUPID, a.STATUS, a.APPSERVER, a.RUN_DATE,a.RUN_TIME,a.USERID,a.HIST_ID,a.NEXTJOBORDER,B.JOB_OUTPUT_MSG_TEMP_CODE,C.ISIMPLICIT from SC_SCHEDULE_REQUEST a,SC_JOB_SCHEDULE_INFO b,SC_M_JOB_GROUP C where A.SCHEDULEID=B.SCHEDULEID AND B.GROUPID = C.GROUPID and ATTEMTCOUNT <5 and a.status='N' and a.APPSERVER='" & strAppServerName & "'" & " and to_date(to_char(RUN_DATE,'DD/MM/YYYY')||' '|| RUN_TIME,'DD/MM/YYYY HH:MI:SS PM') <= "
                    strSql = strSql & "to_date('" & Now.ToShortDateString & " " & Now.ToLongTimeString & "','DD/MM/YYYY HH:MI:SS PM')"
                    cmd = New OracleCommand(strSql)
                Else
                    con = New SqlConnection(strConnection)
                    objAdapt = New SqlDataAdapter
                    strCurDt = "'" & Format(Now.Today, "yyyy-MM-dd").ToString & " " & Now.ToLongTimeString & "'"

                    strSql = "select a.SCREQUESTID, a.SCHEDULEID, a.GROUPID, a.STATUS, a.APPSERVER, a.RUN_DATE,a.RUN_TIME,a.USERID,a.HIST_ID,a.NEXTJOBORDER,B.JOB_OUTPUT_MSG_TEMP_CODE,C.ISIMPLICIT from SC_SCHEDULE_REQUEST a,SC_JOB_SCHEDULE_INFO b,SC_M_JOB_GROUP C where A.SCHEDULEID=B.SCHEDULEID AND B.GROUPID = C.GROUPID"
                    strSql = strSql + "and ATTEMTCOUNT <5 and status='N' and APPSERVER='" & strAppServerName & "'"
                    strSql = strSql + " and Convert(datetime,convert(varchar,run_date,102) +' '+ convert(varchar,Run_time),21) <= convert(datetime," + strCurDt + ",21)"
                    cmd = New SqlClient.SqlCommand(strSql)
                End If

                cmd.Connection = con
                con.Open()
                objAdapt.SelectCommand = cmd
                objAdapt.Fill(dsRequests)
                con.Close()
                objStatus.bStatus = True
                objStatus.objReturn = dsRequests
                Return objStatus
            Else
                objStatus.bStatus = False
                Return objStatus
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection string is nothing", strUserid)
                StopService()
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "RetrievePendingRequests:" & strSql & ex.Message & ex.StackTrace, strUserid)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            RetrievePendingRequests = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function UpdateScheduleRequests(ByVal strUserId As String) As MQSCommonObj.MQSStatus
        'update the Schedule Requests status will be called on start and on stop
        Dim aProcesses As Process
        Dim processID As Integer
        Dim strSql As String
        Dim ds As New DataSet
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim dtNextRun As Date
        Dim systemProcName As String
        Dim systemProc() As String
        Dim timeNextRun As String
        Dim dtTmpDate As String
        Dim dtLastDate As String
        Dim dtModifyDate As String
        Dim strUpdateMsg As String
        Dim i As Integer
        Dim j As Integer
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
                    strSql = "select h.HIST_ID,h.JOB_ID,h.SCHEDULE_ID,h.MESSAGE,h.APPSERVER,h.PRCNT_REC_PROCESSED,h.PROCESSID,j.COMMAND,j.TYPEID,j.PROCESS,T.NAME "
                    strSql = strSql & " from SC_JOB_HISTORY H,SC_M_JOB J,sc_m_job_type T where h.HIST_ID IN(select SCREQUESTID from SC_SCHEDULE_REQUEST where STATUS = 'R' AND APPSERVER='" & strAppServerName & "') AND H.JOB_ID=J.JOBID "
                    strSql = strSql & " and t.JOBTYPEID = j.TYPEID and upper(h.MESSAGE)='RUNNING' order by hist_id "
                Else
                    strSql = "select h.HIST_ID,h.JOB_ID,h.SCHEDULE_ID,h.MESSAGE,h.APPSERVER,h.PRCNT_REC_PROCESSED,h.PROCESSID,j.COMMAND,j.TYPEID,j.PROCESS,T.NAME "
                    strSql = strSql & " from SC_JOB_HISTORY H,SC_M_JOB J,sc_m_job_type T where h.HIST_ID IN(select SCREQUESTID from SC_SCHEDULE_REQUEST where STATUS = 'R' AND APPSERVER='" & strAppServerName & "') AND H.JOB_ID=J.JOBID "
                    strSql = strSql & " and t.JOBTYPEID = j.TYPEID and upper(h.MESSAGE)='RUNNING' order by hist_id "
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBAdapter.SelectCommand = mDBCommand
                mDBAdapter.Fill(ds)
                'for each request which is in running status
                For i = 0 To ds.Tables(0).Rows.Count - 1

                    If Not ds.Tables(0).Rows(i).Item("MESSAGE") Is DBNull.Value Then
                        If ds.Tables(0).Rows(i).Item("NAME").TOUPPER = "BATCHJOBS" Then
                            If ds.Tables(0).Rows(i).Item("MESSAGE").TOUPPER = "RUNNING" Then
                                ExecuteStoredProcedure(ds.Tables(0).Rows(i).Item("HIST_ID"), ds.Tables(0).Rows(i).Item("PROCESS"), strUserId)
                                mDBCommand.Parameters.Clear()
                            End If
                        End If
                    End If
                    If Not ds.Tables(0).Rows(i).Item("PROCESSID") Is DBNull.Value Then
                        processID = ds.Tables(0).Rows(i).Item("PROCESSID")
                        Try
                            'get the proces with process id
                            aProcesses = Process.GetProcessById(processID)
                            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQServices : UpdateScheduleRequest :Try To Kill the Process Id: " & aProcesses.ToString, strUserId)
                            systemProcName = ds.Tables(0).Rows(i).Item("COMMAND")
                            systemProc = systemProcName.Split(".")

                            'check the process name with the command name

                            If aProcesses.ProcessName.ToUpper = systemProc(systemProc.Length - 2).ToUpper Then
                                'kill the windows process
                                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQServices : UpdateScheduleRequest :Try To Kill the Process " & aProcesses.ToString, strUserId)
                                aProcesses.Kill()
                            End If

                        Catch ex As Exception
                            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQServices : UpdateScheduleRequest : " & ex.Message & ex.StackTrace, strUserId)
                        End Try
                    End If
                Next
                'update the history as stopped
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "update sc_job_history set message='Stopped' where hist_id in (select hist_id from SC_SCHEDULE_REQUEST where STATUS='R' AND APPSERVER='" & strAppServerName & "') and (lower(message)<>'completed' and lower(message)<>'failedtorun')"
                Else
                    strSql = "update sc_job_history set message='Stopped' where hist_id in (select hist_id from SC_SCHEDULE_REQUEST where STATUS='R' AND APPSERVER='" & strAppServerName & "') and (lower(message)<>'completed' and lower(message)<>'failedtorun')"
                End If

                mDBCommand.CommandText = strSql
                mDBCommand.CommandType = CommandType.Text
                mDBCommand.ExecuteNonQuery()

                'update the requests as cancled
                If strDbPlatform.ToUpper = "ORACLE" Then
                    strSql = "update SC_SCHEDULE_REQUEST set status='C' where STATUS='R' AND APPSERVER='" & strAppServerName & "'"
                Else
                    strSql = "update SC_SCHEDULE_REQUEST with (UPDLOCK) set status='C' where STATUS='R' AND APPSERVER='" & strAppServerName & "'"
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
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "UpadateScheduleRequests:" & ex.Message & ex.StackTrace & "strSql" & strSql, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            UpdateScheduleRequests = objStatus
            objStatus = Nothing
        End Try
    End Function
    Function CommitTransaction()
        Try
            If Not mDBTransaction Is Nothing Then
                mDBTransaction.Commit()
                mDBConnection.Close()
            End If
            If mDBConnection.State = ConnectionState.Open Then
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
    Function openConnection()
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
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "OpenConnection:" & ex.Message & ex.StackTrace, "PORT")
            objStatus.bStatus = False
            StopService()
        Finally
            openConnection = objStatus
            objStatus = Nothing
        End Try
    End Function
    Private Sub OnTimerElapse(ByVal source As Object, ByVal e As ElapsedEventArgs)
        'on timer elapse this method will be fired
        Dim objStatus As MQSCommonObj.MQSStatus
        Dim intRequestID As Integer
        Dim intScheduleId As Integer
        Dim dsRequests As DataSet
        Dim intI As Integer
        Dim intGoupId As Integer
        Dim strUserid As String
        Dim intHistid As Integer
        Dim blnStatus As Boolean
        Dim strMsgTempCode As String
        Dim strJobOutputType As String
        Dim intIsImplicit As Integer
        Dim intJobOrder As Integer
        Dim dtRundate As Date
        Try
            Dim ci As New System.Globalization.CultureInfo(System.Threading.Thread.CurrentThread.CurrentCulture.LCID)
            ci.DateTimeFormat.ShortDatePattern = "dd/MM/yyyy"
            ci.DateTimeFormat.DateSeparator = "/"
            System.Threading.Thread.CurrentThread.CurrentCulture = ci
            ci.DateTimeFormat.LongTimePattern = "hh:mm:ss tt"

            MQTimer.Enabled = False

            objStatus = RetrievePendingRequests()

            If objStatus.bStatus = False Then
                'write the log to exeception
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "OnTimerElapse:Unable to get the pending requests" & objStatus.strErrDescription, "PORT")
                StopService()
                Exit Sub
            Else
                dsRequests = objStatus.objReturn
                If Not dsRequests Is Nothing Then
                    If dsRequests.Tables(0).Rows.Count > 0 Then
                        'For Each Request create one process
                        For intI = 0 To dsRequests.Tables(0).Rows.Count - 1
                            ' HERE WE NEED TO CHECK WHETHER THE REQUEST IS FOR REPORT OR JOB
                            ' BASED ON THE JOB WE NEED TO CONTRUCT THE PARAMETERS AND PASS TO JOB INSTANCE.
                            strUserid = dsRequests.Tables(0).Rows(intI)("USERID")
                            intRequestID = dsRequests.Tables(0).Rows(intI)("SCREQUESTID")
                            intScheduleId = dsRequests.Tables(0).Rows(intI)("SCHEDULEID")
                            intGoupId = dsRequests.Tables(0).Rows(intI)("GROUPID")
                            strMsgTempCode = dsRequests.Tables(0).Rows(intI)("JOB_OUTPUT_MSG_TEMP_CODE").ToString
                            dtRundate = CDate(dsRequests.Tables(0).Rows(intI)("RUN_DATE"))
                            intIsImplicit = CType(dsRequests.Tables(0).Rows(intI)("ISIMPLICIT"), Integer)
                            intJobOrder = CType(dsRequests.Tables(0).Rows(intI)("NEXTJOBORDER"), Integer)
                            'START THE PROCESS FOR THE GROUP
                            objJob = New JobRequest(strDbPlatform, strConnection, strAppServerName, strSchedulerFilePath, strUserid, intRequestID, intScheduleId, intGoupId, strMsgTempCode, strJobOutputType, intIsImplicit, intJobOrder, strServerIP, dtRundate)
                            'checks wheter to start the job or not
                            objStatus = objJob.CheckStartStatus
                            If objStatus.bStatus = True Then
                                objStatus = objJob.UpdateStatusAsRun()
                                If objStatus.bStatus = True Then
                                    t = New Thread(AddressOf objJob.StartProcess)
                                    t.Start()
                                End If
                            End If
                        Next
                    End If
                End If
            End If
            MQTimer.Enabled = True
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "RausAudit:" & ex.Message & ex.StackTrace, "PORT")
            MQTimer.Enabled = True
        End Try
    End Sub
    'Getting the INI parameter values.
    Private Function getINIParamValues() As MQSCommonObj.MQSStatus
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try
            objStatus = MQSCommonObj.MQSCommon.Initialize()
            If objStatus.bStatus = False Then
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQService: MQService: SetINIParams: Unable to Initialise For Reading the INI Values " & objStatus.strErrDescription, "PORT")
                objStatus.bStatus = False
                objStatus.strErrDescription = objStatus.strErrDescription
                Return objStatus
            End If
            strServerIP = MQSCommonObj.MQSCommon.SMTPServerIP
            strDbPlatform = MQSCommonObj.MQSCommon.DBType
            strConnection = MQSCommonObj.MQSCommon.ConnectionString
            objStatus.bStatus = True
        Catch ex As Exception
            objStatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "MQScheduler:MQService:SetINIParams " & strConnection & ex.Message & ex.StackTrace, "PORT")
        Finally
            getINIParamValues = objStatus
            objStatus = Nothing
        End Try
    End Function
    Private Sub StopService()
        'if dsn or dbserver is not available then stop the service
        Dim i As Long
        Dim services() As System.ServiceProcess.ServiceController
        Dim ServiceName As String
        Try
            MQTimer.Enabled = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Service Stopped", "PORT")
            services = System.ServiceProcess.ServiceController.GetServices()
            ServiceName = "MQScheduler"
            For i = 0 To UBound(services) - 1
                If services(i).DisplayName = ServiceName Then
                    If services(i).CanStop Then
                        If Not services(i).Status = ServiceControllerStatus.Stopped Then
                            services(i).Stop()
                            'services(i).
                            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Service Stopped By Rams", "PORT")
                        End If
                    End If
                    Exit For
                End If
            Next
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "StopService:" & ex.Message & ex.StackTrace, "PORT")
        End Try
    End Sub
    Private Sub StopProcess(ByVal intScheduleId As Integer, ByVal intHistId As Integer, ByVal strJobType As String, ByVal strProcessName As String, ByVal strUserId As String)

        Dim objStatus As MQSCommonObj.MQSStatus
        Dim lstRunDate As Date
        Dim endtime As DateTime
        Dim blnSelected As Boolean
        Dim strSchname As String
        Dim dsSchInfo As DataSet
        Dim startTime As DateTime
        Dim currTime As DateTime
        Dim lngDuration As Long

        Try
            If mDBConnection Is Nothing Then
                objStatus = openConnection()
            ElseIf mDBConnection.State = ConnectionState.Closed Then
                objStatus = openConnection()
            End If
            If objStatus.bStatus = False Then
                Exit Sub
            End If
            If strJobType.ToUpper = "BATCHJOBS" Then
                objStatus = ExecuteStoredProcedure(intHistId, strProcessName, strUserId)
            Else
                'Use scheduler to terminate for normal jobs                                        
            End If
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "StopProcess:" & ex.Message & ex.StackTrace, "PORT")
        End Try
    End Sub
    Function ExecuteStoredProcedure(ByVal iRunId As Integer, ByVal strProcName As String, ByVal strUserId As String) As MQSCommonObj.MQSStatus
        'In case of batch jobs stop execute the stored procedure
        Dim objStatus As New MQSCommonObj.MQSStatus
        Dim i As Int16
        Dim tmpOutParam As IDbDataParameter
        Dim tmpOutParamValid As IDbDataParameter
        Dim tmpOutParamMessage As IDbDataParameter
        Dim objLANG_ID As IDbDataParameter
        Dim objUSERINTERFACE_ID As IDbDataParameter
        Dim objMESSAGE As IDbDataParameter
        Dim objIS_VALIDATION_SUCCESS As IDbDataParameter
        Dim tempParam As IDbDataParameter
        Dim tempProcName As IDbDataParameter
        Try
            If mDBConnection Is Nothing Then
                objStatus = openConnection()
            ElseIf mDBConnection.State = ConnectionState.Closed Then
                objStatus = openConnection()
            End If

            mDBCommand.CommandType = CommandType.StoredProcedure
            mDBCommand.CommandText = "MQSP_UPDATEBATCHRECORDSSTATUS"

            If strDbPlatform.ToUpper = "ORACLE" Then

                tempParam = New OracleParameter
                tempParam.ParameterName = "RUN_ID"
                tempParam.Value = iRunId
                tempParam.Direction = ParameterDirection.Input
                tempParam.DbType = DbType.Int32
                tempProcName = New OracleParameter
                tempProcName.ParameterName = "PROCESS_NAME"
                tempProcName.Value = strProcName
                tempProcName.Direction = ParameterDirection.Input
                tempProcName.DbType = DbType.String
            Else
                tempParam = New SqlParameter
                tempParam.ParameterName = "@" + "RUN_ID"
                tempParam.Value = iRunId
                tempParam.Direction = ParameterDirection.Input
                tempParam.DbType = DbType.Int32
                tempParam.Size = 9
                tempProcName = New SqlParameter
                tempProcName.ParameterName = "@" + "PROCESS_NAME"
                tempProcName.Value = strProcName
                tempProcName.Direction = ParameterDirection.Input
                tempProcName.DbType = DbType.String
                tempProcName.Size = 100
            End If

            objStatus.bStatus = True
            Return objStatus
        Catch ex As Exception
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "ExecuteStoredProcedure:" & ex.Message, strUserId)
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        Finally
            objStatus = Nothing
        End Try
    End Function
End Class