package com.example.lanmesh

import android.Manifest
import android.app.DownloadManager
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.graphics.Color
import android.net.Uri
import android.os.Bundle
import android.os.Environment
import android.os.Handler
import android.os.Looper
import android.webkit.JavascriptInterface
import android.webkit.PermissionRequest
import android.webkit.URLUtil
import android.webkit.ValueCallback
import android.webkit.WebChromeClient
import android.webkit.WebResourceError
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.TextView
import android.widget.Toast
import androidx.activity.OnBackPressedCallback
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import com.google.zxing.BarcodeFormat
import com.google.zxing.qrcode.QRCodeWriter
import com.journeyapps.barcodescanner.ScanContract
import com.journeyapps.barcodescanner.ScanOptions
import mobile.Mobile
import android.webkit.WebStorage

class MainActivity : AppCompatActivity() {

    private val url = "http://127.0.0.1:${LanMeshService.PORT}/"

    private lateinit var web: WebView

    private val ui = Handler(Looper.getMainLooper())

    private var retries = 0

    private var fileCallback: ValueCallback<Array<Uri>>? = null

    private var pendingPermission: PermissionRequest? = null

    /*
     * ------------------------------------------------------------
     * FILE PICKER
     * ------------------------------------------------------------
     */

    private val filePicker =
        registerForActivityResult(
            ActivityResultContracts.StartActivityForResult()
        ) { result ->
            fileCallback?.onReceiveValue(
                WebChromeClient.FileChooserParams.parseResult(
                    result.resultCode,
                    result.data
                )
            )

            fileCallback = null
        }

    /*
     * ------------------------------------------------------------
     * MEDIA PERMISSIONS (MICROPHONE & CAMERA FOR CALLS)
     * ------------------------------------------------------------
     */

    private val mediaPermissionLauncher =
        registerForActivityResult(
            ActivityResultContracts.RequestMultiplePermissions()
        ) { _ ->
            val req = pendingPermission ?: return@registerForActivityResult
            pendingPermission = null

            val grantedResources = mutableListOf<String>()
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO) == PackageManager.PERMISSION_GRANTED) {
                grantedResources.add(PermissionRequest.RESOURCE_AUDIO_CAPTURE)
            }
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) {
                grantedResources.add(PermissionRequest.RESOURCE_VIDEO_CAPTURE)
            }

            if (grantedResources.isNotEmpty()) {
                req.grant(grantedResources.toTypedArray())
            } else {
                req.deny()
            }
        }

    /*
     * ------------------------------------------------------------
     * CAMERA PERMISSION
     * ------------------------------------------------------------
     */

    private val cameraPermissionLauncher =
        registerForActivityResult(
            ActivityResultContracts.RequestPermission()
        ) { granted ->

            if (granted) {
                launchQRScanner()
            } else {
                Toast.makeText(
                    this,
                    "Camera permission is required to scan QR codes",
                    Toast.LENGTH_SHORT
                ).show()
            }
        }

    /*
     * ------------------------------------------------------------
     * QR SCANNER
     * ------------------------------------------------------------
     */

    private val scanLauncher =
        registerForActivityResult(
            ScanContract()
        ) { result ->

            val contents = result.contents

            if (!contents.isNullOrBlank()) {
                connectToPeer(contents)
            }
        }

    /*
     * ------------------------------------------------------------
     * JAVASCRIPT BRIDGE
     * ------------------------------------------------------------
     */

    inner class Bridge {

        /*
         * Existing clear-data functionality
         */
        @JavascriptInterface
        fun clearData() {

            runOnUiThread {

                stopService(
                    Intent(
                        this@MainActivity,
                        LanMeshService::class.java
                    )
                )

                Thread {

                    var waited = 0

                    while (Mobile.isRunning() && waited < 50) {
                        Thread.sleep(100)
                        waited++
                    }

                    filesDir.listFiles()?.forEach {
                        it.deleteRecursively()
                    }

                    runOnUiThread {

                        WebStorage.getInstance().deleteAllData()

                        retries = 0

                        ContextCompat.startForegroundService(
                            this@MainActivity,
                            Intent(
                                this@MainActivity,
                                LanMeshService::class.java
                            )
                        )

                        ui.postDelayed({
                            web.loadUrl(url)
                        }, 1500)
                    }

                }.start()
            }
        }

        /*
         * Connect to another LanMesh peer.
         */
        @JavascriptInterface
        fun connectToPeer(address: String) {

            this@MainActivity.connectToPeer(address)
        }

        /*
         * Show this device's peer address as a QR code.
         */
        @JavascriptInterface
        fun showQR() {

            this@MainActivity.showQR()
        }

        /*
         * Start QR scanner.
         */
        @JavascriptInterface
        fun scanQR() {

            this@MainActivity.checkCameraAndScan()
        }
    }

    /*
     * ------------------------------------------------------------
     * ACTIVITY
     * ------------------------------------------------------------
     */

    override fun onCreate(savedInstanceState: Bundle?) {

        super.onCreate(savedInstanceState)

        supportActionBar?.hide()

        /*
         * Original LanMesh WebView
         */
        web = WebView(this)

        setContentView(web)

        WebView.setWebContentsDebuggingEnabled(true)

        web.settings.apply {

            javaScriptEnabled = true

            domStorageEnabled = true

            mediaPlaybackRequiresUserGesture = false
        }

        /*
         * --------------------------------------------------------
         * WEBVIEW CLIENT
         * --------------------------------------------------------
         */

        web.webViewClient = object : WebViewClient() {

            override fun onReceivedError(
                view: WebView,
                request: WebResourceRequest,
                error: WebResourceError
            ) {

                if (request.isForMainFrame && retries++ < 20) {

                    ui.postDelayed({

                        view.loadUrl(url)

                    }, 750)
                }
            }
        }

        /*
         * --------------------------------------------------------
         * WEB CHROME CLIENT
         * --------------------------------------------------------
         */

        web.webChromeClient = object : WebChromeClient() {

            /*
             * File uploads
             */
            override fun onShowFileChooser(
                view: WebView,
                callback: ValueCallback<Array<Uri>>,
                params: FileChooserParams
            ): Boolean {

                fileCallback?.onReceiveValue(null)

                fileCallback = callback

                filePicker.launch(params.createIntent())

                return true
            }

            /*
             * Microphone and Camera permissions for WebRTC calls
             */
            override fun onPermissionRequest(
                request: PermissionRequest
            ) {
                runOnUiThread {
                    val requestedResources = request.resources
                    val needsAudio = PermissionRequest.RESOURCE_AUDIO_CAPTURE in requestedResources
                    val needsVideo = PermissionRequest.RESOURCE_VIDEO_CAPTURE in requestedResources

                    if (!needsAudio && !needsVideo) {
                        request.deny()
                        return@runOnUiThread
                    }

                    val hasAudioPermission = ContextCompat.checkSelfPermission(
                        this@MainActivity,
                        Manifest.permission.RECORD_AUDIO
                    ) == PackageManager.PERMISSION_GRANTED

                    val hasCameraPermission = ContextCompat.checkSelfPermission(
                        this@MainActivity,
                        Manifest.permission.CAMERA
                    ) == PackageManager.PERMISSION_GRANTED

                    val grantedList = mutableListOf<String>()
                    val permissionsToAsk = mutableListOf<String>()

                    if (needsAudio) {
                        if (hasAudioPermission) grantedList.add(PermissionRequest.RESOURCE_AUDIO_CAPTURE)
                        else permissionsToAsk.add(Manifest.permission.RECORD_AUDIO)
                    }

                    if (needsVideo) {
                        if (hasCameraPermission) grantedList.add(PermissionRequest.RESOURCE_VIDEO_CAPTURE)
                        else permissionsToAsk.add(Manifest.permission.CAMERA)
                    }

                    if (permissionsToAsk.isEmpty()) {
                        request.grant(grantedList.toTypedArray())
                    } else {
                        pendingPermission = request
                        mediaPermissionLauncher.launch(permissionsToAsk.toTypedArray())
                    }
                }
            }
        }

        /*
         * --------------------------------------------------------
         * DOWNLOAD HANDLER
         * --------------------------------------------------------
         */

        web.setDownloadListener {
                dlUrl,
                userAgent,
                contentDisposition,
                mimeType,
                _ ->

            if (!dlUrl.startsWith("http")) {

                Toast.makeText(
                    this,
                    "Unsupported download type",
                    Toast.LENGTH_SHORT
                ).show()

                return@setDownloadListener
            }

            val name = URLUtil.guessFileName(
                dlUrl,
                contentDisposition,
                mimeType
            )

            val request =
                DownloadManager.Request(Uri.parse(dlUrl))
                    .setMimeType(mimeType)
                    .addRequestHeader(
                        "User-Agent",
                        userAgent
                    )
                    .setNotificationVisibility(
                        DownloadManager.Request
                            .VISIBILITY_VISIBLE_NOTIFY_COMPLETED
                    )
                    .setDestinationInExternalPublicDir(
                        Environment.DIRECTORY_DOWNLOADS,
                        name
                    )

            getSystemService(
                DownloadManager::class.java
            ).enqueue(request)

            Toast.makeText(
                this,
                "Downloading $name",
                Toast.LENGTH_SHORT
            ).show()
        }

        /*
         * --------------------------------------------------------
         * BACK BUTTON
         * --------------------------------------------------------
         */

        onBackPressedDispatcher.addCallback(
            this,
            object : OnBackPressedCallback(true) {

                override fun handleOnBackPressed() {

                    if (web.canGoBack()) {

                        web.goBack()

                    } else {

                        moveTaskToBack(true)
                    }
                }
            }
        )

        /*
         * --------------------------------------------------------
         * JAVASCRIPT BRIDGE
         * --------------------------------------------------------
         *
         * This is what allows index.html to call:
         *
         * AndroidApp.clearData()
         * AndroidApp.connectToPeer(...)
         * AndroidApp.showQR()
         * AndroidApp.scanQR()
         */

        web.addJavascriptInterface(
            Bridge(),
            "AndroidApp"
        )

        /*
         * Load original LanMesh interface
         */
        web.loadUrl(url)
    }

    /*
     * ------------------------------------------------------------
     * MANUAL CONNECTION
     * ------------------------------------------------------------
     */

    private fun connectToPeer(address: String) {

        val cleanAddress = address.trim()

        if (cleanAddress.isEmpty()) {

            Toast.makeText(
                this,
                "Enter a peer address first",
                Toast.LENGTH_SHORT
            ).show()

            return
        }

        if (cleanAddress.contains("/127.0.0.1/") || cleanAddress.contains("/::1/") || cleanAddress.contains("/0.0.0.0/")) {

            Toast.makeText(
                this,
                "Cannot connect: address contains loopback/unroutable IP (127.0.0.1). Ensure remote peer provides its LAN address.",
                Toast.LENGTH_LONG
            ).show()

            return
        }

        Thread {

            try {

                Mobile.connectToPeer(cleanAddress)

                runOnUiThread {

                    Toast.makeText(
                        this,
                        "Connected to peer",
                        Toast.LENGTH_SHORT
                    ).show()
                }

            } catch (e: Exception) {

                runOnUiThread {

                    Toast.makeText(
                        this,
                        "Connection failed: ${e.message}",
                        Toast.LENGTH_LONG
                    ).show()
                }
            }

        }.start()
    }

    /*
     * ------------------------------------------------------------
     * SHOW MY QR
     * ------------------------------------------------------------
     */

    private fun showQR() {

        Thread {

            try {

                val currentIp = LanMeshService.getLocalIpAddress(this@MainActivity)
                if (!currentIp.isNullOrBlank()) {
                    Mobile.setLocalIP(currentIp)
                }

                val addresses = Mobile.myAddresses()

                /*
                 * Use the first available LAN address (strictly excluding loopback/wildcard).
                 *
                 * Example:
                 * /ip4/192.168.1.10/tcp/45343/p2p/12D3KooW...
                 */
                val address =
                    addresses
                        .lines()
                        .firstOrNull { line ->
                            line.isNotBlank() &&
                                !line.contains("/127.0.0.1/") &&
                                !line.contains("/0.0.0.0/") &&
                                !line.contains("/::1/")
                        }

                if (address.isNullOrBlank()) {

                    runOnUiThread {

                        Toast.makeText(
                            this,
                            "No reachable LAN address available. Ensure this phone is connected to Wi-Fi/Hotspot.",
                            Toast.LENGTH_LONG
                        ).show()
                    }

                    return@Thread
                }

                val writer = QRCodeWriter()

                val size = 700

                val matrix = writer.encode(
                    address,
                    BarcodeFormat.QR_CODE,
                    size,
                    size
                )

                val bitmap =
                    Bitmap.createBitmap(
                        size,
                        size,
                        Bitmap.Config.RGB_565
                    )

                for (x in 0 until size) {

                    for (y in 0 until size) {

                        bitmap.setPixel(
                            x,
                            y,
                            if (matrix[x, y]) {
                                Color.BLACK
                            } else {
                                Color.WHITE
                            }
                        )
                    }
                }

                runOnUiThread {

                    val container =
                        LinearLayout(this).apply {

                            orientation =
                                LinearLayout.VERTICAL

                            setPadding(
                                30,
                                10,
                                30,
                                10
                            )
                        }

                    val imageView =
                        ImageView(this).apply {

                            setImageBitmap(bitmap)

                            adjustViewBounds = true
                        }

                    val addressView =
                        TextView(this).apply {

                            text = address

                            textSize = 11f

                            setPadding(
                                10,
                                15,
                                10,
                                5
                            )

                            setTextIsSelectable(true)
                        }

                    container.addView(imageView)

                    container.addView(addressView)

                    AlertDialog.Builder(this)
                        .setTitle("My LanMesh Address")
                        .setView(container)
                        .setPositiveButton(
                            "Close",
                            null
                        )
                        .show()
                }

            } catch (e: Exception) {

                runOnUiThread {

                    Toast.makeText(
                        this,
                        "QR generation failed: ${e.message}",
                        Toast.LENGTH_LONG
                    ).show()
                }
            }

        }.start()
    }

    /*
     * ------------------------------------------------------------
     * QR SCANNING
     * ------------------------------------------------------------
     */

    private fun checkCameraAndScan() {

        val granted =
            ContextCompat.checkSelfPermission(
                this,
                Manifest.permission.CAMERA
            ) == PackageManager.PERMISSION_GRANTED

        if (granted) {

            launchQRScanner()

        } else {

            cameraPermissionLauncher.launch(
                Manifest.permission.CAMERA
            )
        }
    }

    private fun launchQRScanner() {

        val options = ScanOptions()

        options.setDesiredBarcodeFormats(
            ScanOptions.QR_CODE
        )

        options.setPrompt(
            "Scan the other device's LanMesh QR code"
        )

        options.setBeepEnabled(false)

        options.setOrientationLocked(true)

        scanLauncher.launch(options)
    }

    /*
     * ------------------------------------------------------------
     * RESUME
     * ------------------------------------------------------------
     */

    override fun onResume() {

        super.onResume()

        if (!Mobile.isRunning()) {

            ContextCompat.startForegroundService(
                this,
                Intent(
                    this,
                    LanMeshService::class.java
                )
            )

            retries = 0

            web.loadUrl(url)
        } else {
            web.evaluateJavascript("if (typeof connectWS === 'function') connectWS();", null)
        }
    }

    /*
     * ------------------------------------------------------------
     * DESTROY
     * ------------------------------------------------------------
     */

    override fun onDestroy() {

        ui.removeCallbacksAndMessages(null)

        web.destroy()

        super.onDestroy()
    }
}