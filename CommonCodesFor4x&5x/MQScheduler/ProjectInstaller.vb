Imports System.ComponentModel
Imports System.Configuration.Install

<RunInstaller(True)> Public Class ProjectInstaller
    Inherits System.Configuration.Install.Installer

#Region " Component Designer generated code "

    Public Sub New()
        MyBase.New()

        'This call is required by the Component Designer.
        InitializeComponent()

        'Add any initialization after the InitializeComponent() call

    End Sub

    'Installer overrides dispose to clean up the component list.
    Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
        If disposing Then
            If Not (components Is Nothing) Then
                components.Dispose()
            End If
        End If
        MyBase.Dispose(disposing)
    End Sub

    'Required by the Component Designer
    Private components As System.ComponentModel.IContainer

    'NOTE: The following procedure is required by the Component Designer
    'It can be modified using the Component Designer.  
    'Do not modify it using the code editor.
    Friend WithEvents MQServiceProcessInstaller As System.ServiceProcess.ServiceProcessInstaller
    Friend WithEvents MQServiceInstaller As System.ServiceProcess.ServiceInstaller
    <System.Diagnostics.DebuggerStepThrough()> Private Sub InitializeComponent()
        Me.MQServiceProcessInstaller = New System.ServiceProcess.ServiceProcessInstaller
        Me.MQServiceInstaller = New System.ServiceProcess.ServiceInstaller
        '
        'MQServiceProcessInstaller
        '
        Me.MQServiceProcessInstaller.Account = System.ServiceProcess.ServiceAccount.LocalSystem
        Me.MQServiceProcessInstaller.Password = Nothing
        Me.MQServiceProcessInstaller.Username = Nothing
        '
        'MQServiceInstaller
        '
        Me.MQServiceInstaller.ServiceName = "MQScheduler"
        Me.MQServiceInstaller.StartType = System.ServiceProcess.ServiceStartMode.Automatic
        '
        'ProjectInstaller
        '
        Me.Installers.AddRange(New System.Configuration.Install.Installer() {Me.MQServiceProcessInstaller, Me.MQServiceInstaller})

    End Sub

#End Region

End Class
