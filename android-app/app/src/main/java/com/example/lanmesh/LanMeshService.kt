package com.example.lanmesh

import android.app.*
import android.content.Context
import android.content.Intent
import android.net.wifi.WifiManager
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat
import mobile.Mobile

class LanMeshService : Service() {

    companion object {
        const val ACTION_STOP = "com.example.lanmesh.STOP"
        const val PORT = 9000L
        private const val CHANNEL_ID = "lanmesh_node"
        private const val NOTIF_ID = 1
        private const val TAG = "LanMeshService"

        fun getLocalIpAddress(context: Context): String? {
            // 1. Try ConnectivityManager (modern, official Android API)
            try {
                val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? android.net.ConnectivityManager
                val activeNetwork = cm?.activeNetwork
                val linkProps = cm?.getLinkProperties(activeNetwork)
                val ip = linkProps?.linkAddresses?.mapNotNull { it.address }?.firstOrNull {
                    it is java.net.Inet4Address && !it.isLoopbackAddress && !it.isLinkLocalAddress
                }?.hostAddress
                if (!ip.isNullOrBlank()) {
                    Log.i(TAG, "Found LAN IP via ConnectivityManager: $ip")
                    return ip
                }
            } catch (e: Exception) {
                Log.w(TAG, "ConnectivityManager getLinkProperties error: ${e.message}")
            }

            // 2. Try java.net.NetworkInterface (fallback for hotspot or older APIs)
            try {
                val interfaces = java.net.NetworkInterface.getNetworkInterfaces()
                if (interfaces != null) {
                    for (iface in interfaces) {
                        if (!iface.isUp || iface.isLoopback) continue
                        for (addr in iface.inetAddresses) {
                            if (addr is java.net.Inet4Address && !addr.isLoopbackAddress && !addr.isLinkLocalAddress) {
                                val host = addr.hostAddress
                                if (!host.isNullOrBlank() && host != "127.0.0.1") {
                                    Log.i(TAG, "Found LAN IP via NetworkInterface(${iface.name}): $host")
                                    return host
                                }
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                Log.w(TAG, "NetworkInterface getNetworkInterfaces error: ${e.message}")
            }

            return null
        }
    }

    private var multicastLock: WifiManager.MulticastLock? = null
    private var wifiLock: WifiManager.WifiLock? = null
    private var worker: Thread? = null

    override fun onCreate() {
        super.onCreate()
        val nm = getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(
            NotificationChannel(CHANNEL_ID, "LanMesh node", NotificationManager.IMPORTANCE_LOW)
        )
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) {
            stopSelf()
            return START_NOT_STICKY
        }

        startForeground(NOTIF_ID, buildNotification("Starting..."))
        acquireLocks()

        if (worker?.isAlive == true) return START_STICKY

        worker = Thread {
            try {
                val localIp = getLocalIpAddress(applicationContext)
                if (!localIp.isNullOrBlank()) {
                    Mobile.setLocalIP(localIp)
                    Log.i(TAG, "Registered local IP with Go: $localIp")
                }
                if (!Mobile.isRunning()) Mobile.start(PORT, filesDir.absolutePath)
            } catch (e: Exception) {
                Log.e(TAG, "node start failed", e)
                stopSelf()
                return@Thread
            }
            val nm = getSystemService(NotificationManager::class.java)
            while (!Thread.currentThread().isInterrupted) {
                val currentIp = getLocalIpAddress(applicationContext)
                if (!currentIp.isNullOrBlank()) {
                    Mobile.setLocalIP(currentIp)
                }
                val n = Mobile.peers().lines().count { it.isNotBlank() }
                nm.notify(NOTIF_ID, buildNotification("Running · $n peer(s)"))
                try { Thread.sleep(5000) } catch (_: InterruptedException) { break }
            }
        }.also { it.start() }

        return START_STICKY
    }

    override fun onDestroy() {
        worker?.interrupt()
        try { Mobile.stop() } catch (e: Exception) { Log.e(TAG, "stop failed", e) }
        multicastLock?.let { if (it.isHeld) it.release() }
        wifiLock?.let { if (it.isHeld) it.release() }
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    @Suppress("DEPRECATION")
    private fun acquireLocks() {
        val wifi = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
        if (multicastLock?.isHeld != true) {
            multicastLock = wifi.createMulticastLock("lanmesh").apply {
                setReferenceCounted(false); acquire()
            }
        }
        if (wifiLock?.isHeld != true) {
            wifiLock = wifi.createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, "lanmesh").apply {
                setReferenceCounted(false); acquire()
            }
        }
    }

    private fun buildNotification(text: String): Notification {
        val open = PendingIntent.getActivity(
            this, 0, Intent(this, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE
        )
        val stop = PendingIntent.getService(
            this, 1,
            Intent(this, LanMeshService::class.java).setAction(ACTION_STOP),
            PendingIntent.FLAG_IMMUTABLE
        )
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setContentTitle("LanMesh")
            .setContentText(text)
            .setOngoing(true)
            .setContentIntent(open)
            .addAction(0, "Stop", stop)
            .build()
    }
}