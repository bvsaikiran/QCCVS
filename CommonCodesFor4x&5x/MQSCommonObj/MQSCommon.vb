Imports System.Web
Imports System.Resources
Imports System.IO
Public Class MQSCommon
    Shared strConnectionString As String
    Shared strDBType As String
    Shared strServerIP As String
    Shared strConnectionTimeout As Integer
    Shared strReportDSN As String
    Shared Function GetMessage(ByVal lngMessageId As Long, ByVal ParamArray vntParamArr() As Object) As Object
        'On Error Resume Next
        Dim strMessage As String
        Dim strMsgs() As String
        Dim intI As Integer

        Try

            Dim objResource As System.Reflection.Assembly = System.Reflection.Assembly.Load("MQSResource")

            Dim objCustomResourceMgr As ResourceManager
            objCustomResourceMgr = New ResourceManager("customMessages", objResource)

            Dim objResMgr As ResourceManager
            objResMgr = New ResourceManager("Messages", objResource)
            strMessage = objCustomResourceMgr.GetString(lngMessageId)

            If Len(strMessage) = 0 Then
                strMessage = objResMgr.GetString(lngMessageId)
            End If

            If Len(strMessage) = 0 Then
                strMessage = "System error occured. Please contact system administrator."
            ElseIf IsArray(vntParamArr) = True Then
                If vntParamArr.Length > 0 Then
                    If IsArray(vntParamArr(0)) Then
                        Dim temp As Object
                        temp = vntParamArr(0)
                        vntParamArr = temp
                    End If
                    For intI = 1 To UBound(vntParamArr) + 1
                        strMessage = Replace(strMessage, "%" & intI & "%", vntParamArr(intI - 1))
                    Next
                    strMsgs = Split(strMessage, "%B%")
                    For intI = 0 To UBound(strMsgs)
                        If (intI = 0) Then
                            strMessage = strMsgs(intI)
                        Else
                            strMessage = strMessage & vbCrLf & strMsgs(intI)
                        End If
                    Next
                End If
            End If
            Return strMessage
        Catch ex As Exception
            'Throw ex
            'TODO:Handle the Exception properly
            Return strMessage
        End Try
    End Function
    Shared Function Initialize() As MQSCommonObj.MQSStatus
        Dim objStatus As New MQSCommonObj.MQSStatus
        Try
            objStatus = GetINIParameterValues()
            If objStatus.bStatus = False Then
                Return objStatus
            Else
                objStatus.bStatus = True
                Return objStatus
            End If
        Catch ex As Exception
            objStatus.bStatus = False
            objStatus.strErrDescription = ex.Message
            Return objStatus
        End Try
    End Function

    Private Shared Function GetINIParameterValues() As MQSCommonObj.MQSStatus
        Dim objStatus As New MQSStatus
        Dim strINIFile As String
        Dim strLine As String
        Dim strServerIP As String
        Dim strReportDSN As String

        Dim bServerIP As Boolean = False
        Dim bConnTimeout As Boolean = False
        Dim bDSN As Boolean = False
        Dim bEncrypt As Boolean = False
        Dim bReportDSN As Boolean = False


        Dim strTemp() As String
        Dim strpass() As String
        Dim strFilePath As String
        Dim strDSN As String
        Dim strData As String
        Dim strProvider As String
        Dim strDataSource As String
        Dim strUserId As String
        Dim CryptoDSNkey As String = "MQS393DsnPwd"
        Dim DSN As String
        Dim strPassword As String
        Dim bPassEncrypt As Boolean = False
        Dim i As Int16
        Dim intStatus As Int16

        Dim strOledbConnection As String = String.Empty
        Dim objCrypto As MQCRYPTOLib.MQEncDecClass

        Try
            'objLib = New MQSLib.MqsCommon
            'objLib.Initialize()
            'strOledbConnection = objLib.GetDsn()
            'strServerIP = objLib.strServerIP

            strINIFile = Environment.SystemDirectory & "/mqs.ini"
            Dim fs As FileStream = New FileStream(strINIFile, FileMode.Open, FileAccess.Read)
            Dim sr As StreamReader = New StreamReader(fs)
            sr.BaseStream.Seek(0, SeekOrigin.Begin)

            While sr.Peek() > -1
                strLine = sr.ReadLine

                If strLine.ToUpper.Trim.StartsWith("DB_PASSWORD_ENCRYPTION") Then
                    If strLine.Split(":")(1).ToUpper.Equals("TRUE") Then
                        'MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Password is Encrypted", "PORT")
                        bEncrypt = True
                        bPassEncrypt = True
                    End If
                End If

                If strLine.ToUpper.Trim.StartsWith("DSN") Then
                    strOledbConnection = strLine.Split(":")(1)
                    strDSN = strLine
                    If strOledbConnection.ToUpper.Trim.IndexOf("SQLOLEDB") > 0 Then
                        strDBType = "SQLSERVER"
                    Else
                        strDBType = "ORACLE"
                    End If
                    bDSN = True
                End If
                If strLine.ToUpper.Trim.StartsWith("SERVERIP") Then
                    strServerIP = strLine.Split(":")(1)
                    bServerIP = True
                End If

                If strLine.ToUpper.Trim.StartsWith("REPORTDSN") Then
                    strReportDSN = strLine.Split(":")(1)
                    bReportDSN = True
                End If

                If strLine.ToUpper.Trim.StartsWith("CONNECTIONTIMEOUT") Then
                    strConnectionTimeout = strLine.Split(":")(1)
                    bConnTimeout = True
                End If
                If bDSN And bServerIP And bPassEncrypt And bConnTimeout And bReportDSN Then
                    Exit While
                End If
            End While


            If bEncrypt = True Then
                strTemp = strDSN.Split(";")
                'MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Try To get Other informations from DSN", "PORT")
                For i = 0 To strTemp.Length - 1
                    If strTemp(i).ToUpper.Trim.StartsWith("PASSWORD") Then
                        strFilePath = strTemp(i).Split("=")(1)
                    End If
                    If strTemp(i).ToUpper.Trim.StartsWith("DSN") Then
                        strProvider = strTemp(i).Split(":")(1)
                    End If

                    If strTemp(i).ToUpper.Trim.StartsWith("USER ID") Then
                        strUserId = strTemp(i)
                    End If
                    If strTemp(i).ToUpper.Trim.StartsWith("DATA SOURCE") Then
                        strDataSource = strTemp(i)
                    End If
                Next

                'MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Before Creating The Crypto Object", "PORT")

                objCrypto = New MQCRYPTOLib.MQEncDecClass

                'MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "After Creating The Crypto Object and Before Call Decrypt method", "PORT")

                intStatus = objCrypto.DecryptToFile(strFilePath, CryptoDSNkey, strData)

                'MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "After calling the Decrpt Method", "PORT")
                If intStatus = 0 Then
                    'MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Decrypted Data :" + strData, "PORT")

                    strPassword = "Password=" + strData
                    ' DSN = strProvider + ";" + strUserId + ";" + strPassword + ";" + strDataSource
                    DSN = strUserId + ";" + strPassword + ";" + strDataSource
                    strConnectionString = DSN
                    'MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Connection String " + strConnection, "PORT")
                Else
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Unable To Decrypt ", "PORT")
                End If
            Else
                If bDSN Then
                    strConnectionString = ""
                    Dim str As String = strOledbConnection
                    Dim strArr() As String = Split(str, ";")
                    For Each str In strArr
                        If Split(str, "=")(0).ToLower <> "provider" Then strConnectionString = strConnectionString & str & ";"
                    Next
                Else
                    objStatus.bStatus = False
                    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Unable to Get the DSN", "PORT")
                    Exit Function
                End If
            End If
            'MQSCommonObj.MQSCommon.

            'If bDSN Then
            '    strConnection = ""
            '    Dim str As String = strOledbConnection
            '    Dim strArr() As String = Split(str, ";")
            '    For Each str In strArr
            '        If Split(str, "=")(0).ToLower <> "provider" Then strConnection = strConnection & str & ";"
            '    Next
            'Else
            '    objStatus.bStatus = False
            '    MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Unable to Get the DSN", "PORT")
            '    Exit Function
            'End If
            If Not bServerIP Then
                objStatus.bStatus = False
                MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Unable to Get the SMTP Server IP", "PORT")
                Exit Function
            End If
            sr.Close()
            objStatus.bStatus = True
            'MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Informations :SetINIParams: values " & strConnection & " " & strServerIP, "PORT")
        Catch ex As Exception
            objStatus.bStatus = False
            MQSCommonObj.MQSEventLog.logMesg(MQSCommonObj.MQSEventLog.LogLevel.ErrorLevel, "Execption :SetINIParams " & strConnectionString & ex.Message & ex.StackTrace, "PORT")
        Finally
            GetINIParameterValues = objStatus
            objStatus = Nothing
        End Try
    End Function
    Public Shared ReadOnly Property ConnectionString() As String
        Get
            Return strConnectionString
        End Get
    End Property
    Public Shared ReadOnly Property DBType() As String
        Get
            Return strDBType
        End Get
    End Property
    Public Shared ReadOnly Property SMTPServerIP() As String
        Get
            Return strServerIP
        End Get
    End Property
    Public Shared ReadOnly Property ConnectionTimeout() As String
        Get
            Return strConnectionTimeout
        End Get
    End Property
    Public Shared ReadOnly Property ReportDSN() As String
        Get
            Return strReportDSN
        End Get
    End Property
End Class
