Imports System.Configuration

Public Class MQSEventLog
    ' Private Shared BITraceLevel As New TraceSwitch( _
    ' "TraceExample", "Trace Example Trace Level")
    Private Shared mqsTraceListener As New EventLogTraceListener("MQSubscribe")

    Public Enum LogLevel
        InfoLevel = 5
        DebugLevel = 4
        WarningLevel = 3
        ErrorLevel = 2
        FatalLevel = 1
    End Enum


    Public Shared Sub logMesg(ByVal iLogLevel As Integer, ByVal strMessage As String, ByVal strUserId As String)
        writeLogMesg(iLogLevel, strMessage, strUserId, "MQSubscribe")
    End Sub
    Public Shared Sub logMesg(ByVal iLogLevel As Integer, ByVal strMessage As String, ByVal strUserId As String, ByVal strCategory As String)
        writeLogMesg(iLogLevel, strMessage, strUserId, strCategory)
    End Sub
    '**
    'WriteLogMesg will log the message into eventlog
    Private Shared Sub writeLogMesg(ByVal iLogLevel As Integer, ByVal strMessage As String, ByVal strUserId As String, ByVal strCategory As String)
        Try
            If Not (EventLog.Exists("MQSLog") And EventLog.SourceExists("MQSubscribe")) Then
                EventLog.CreateEventSource("MQSubscribe", "MQSLog")
            End If
            ' Add the event log trace listener to the collection.
            Trace.Listeners.Add(mqsTraceListener)

            'here 4 is the application log level - need to get it from app config
            ' Write output to the event log.
            If iLogLevel <= 4 Then
                mqsTraceListener.WriteLine("UserId: " & strUserId & "   " & strMessage, strCategory)
            End If
            'if log level is error or fatal, flush immediately
            If iLogLevel = LogLevel.ErrorLevel Or iLogLevel = LogLevel.FatalLevel Then
                mqsTraceListener.Flush()
            End If
        Catch ex As Exception
            Throw ex
        End Try
    End Sub
End Class

