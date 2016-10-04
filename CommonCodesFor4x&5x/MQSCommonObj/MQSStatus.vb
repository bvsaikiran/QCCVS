'This class represents MQS status object.
Public Class MQSStatus
    Private m_bStatus As Boolean
    Private m_lngErrorNo As Long
    Private m_strErrDescription As String
    Private m_iErrorLevel As Integer
    Private m_Parameters As Object
    Private m_objReturn As Object
    Private m_lngMessageType As Long

    Public Property bStatus() As Boolean
        Get
            Return m_bStatus
        End Get
        Set(ByVal Value As Boolean)
            m_bStatus = Value
        End Set
    End Property
    Public Property lngErrorNo() As Long
        Get
            Return m_lngErrorNo
        End Get
        Set(ByVal Value As Long)
            m_lngErrorNo = Value
        End Set
    End Property
    Public Property strErrDescription() As String
        Get
            Return m_strErrDescription
        End Get
        Set(ByVal Value As String)
            m_strErrDescription = Value
        End Set
    End Property
    Public Property iErrorLevel() As Integer
        Get
            Return m_iErrorLevel
        End Get
        Set(ByVal Value As Integer)
            m_iErrorLevel = Value
        End Set
    End Property
    Public Property objReturn() As Object
        Get
            Return m_objReturn
        End Get
        Set(ByVal Value As Object)
            m_objReturn = Value
        End Set
    End Property
    'Constructor: Initializes status object
    Public Sub New()
        m_bStatus = True
        m_lngErrorNo = 0
        m_strErrDescription = ""
        m_iErrorLevel = MQSEventLog.LogLevel.InfoLevel
        m_objReturn = Nothing
    End Sub
    Public Property lngmessageType() As Long
        Get
            Return m_lngMessageType
        End Get
        Set(ByVal Value As Long)
            m_lngMessageType = Value
        End Set
    End Property
    Public Property MsgParameter() As Object
        Get
            Return (m_Parameters)
        End Get
        Set(ByVal Value As Object)
            m_Parameters = Value
        End Set
    End Property
End Class
