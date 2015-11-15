import "dart:async";
import "dart:convert";
import "dart:io";

import "dart:typed_data";

import "package:dslink/dslink.dart";
import "package:dslink/nodes.dart";
import "package:crypto/crypto.dart";

import "package:watcher/watcher.dart";
import "package:http/http.dart" as http;
import "package:watcher/src/resubscribable.dart";

LinkProvider link;

String hashString(String input) => CryptoUtils.bytesToHex((
    new SHA1()..add(UTF8.encode(input))
).close());

List<FileNode> fileNodes = [];
List<HttpNode> httpNodes = [];

class FileNode extends SimpleNode {
  File file;
  StreamSubscription sub;

  FileNode(String path) : super(path);

  bool isBinary = false;

  @override
  onCreated() {
    gp(a) {
      var ep = path + "/" + a;
      if (ep.startsWith("//")) ep = ep.substring(1);
      return ep;
    }

    configs[r"$writable"] = "write";
    var filePath = attributes["@filePath"];

    if (filePath == null) {
      link.removeNode(path);
      return;
    }

    if (attributes["@fileBinary"] == true) {
      isBinary = true;
    }

    file = new File(filePath);
    configs[r"$type"] = isBinary ? "binary" : "string";

    link.addNode(gp("remove"), REMOVE_ACTION);

    if (isBinary) {
      link.addNode(gp("readBinaryData"), READ_BINARY_DATA);
    }

    if (!fileNodes.contains(this)) {
      fileNodes.add(this);
    }

    updateList(r"$is");
  }

  @override
  onSubscribe() {
    subs++;
    checkSubscriptions();
  }

  @override
  onUnsubscribe() {
    subs--;
    checkSubscriptions();
  }

  checkSubscriptions() async {
    if (subs < 0) {
      subs = 0;
    }

    if (subs == 0) {
      if (sub != null) {
        sub.cancel();
      }
      clearValue();
    } else {
      await loadValue();
      await subscribeToWatcher();
    }
  }

  subscribeToWatcher() async {
    if (sub != null) {
      sub.cancel();
    }

    try {
      if (watcher is ManuallyClosedWatcher) {
        (watcher as ManuallyClosedWatcher).close();
      }
      watcher = new FileWatcher(file.path);

      sub = watcher.events.listen((WatchEvent event) async {
        await loadValue();

        if (event.type == ChangeType.REMOVE) {
          await subscribeToWatcher();
        }
      });
    } catch (e) {
      new Future.delayed(const Duration(seconds: 1), subscribeToWatcher);
    }
  }

  loadValue() async {
    try {
      if (!(await file.exists())) {
        updateValue(null);
        return;
      }

      var lastModified = await file.lastModified();

      String ts;
      ts = "${lastModified.toIso8601String()}${ValueUpdate.TIME_ZONE}";

      if (isBinary) {
        Uint8List list;
        List<int> bytes = await file.readAsBytes();
        if (bytes is Uint8List) {
          list = bytes;
        } else {
          list = new Uint8List.fromList(bytes);
        }

        var update = new ValueUpdate(list.buffer.asByteData(), ts: ts);
        updateValue(update);
      } else {
        try {
          var update = new ValueUpdate(await file.readAsString(), ts: ts);
          updateValue(update);
        } on FormatException catch (_) {
          var bytes = await file.readAsBytes();
          var update = new ValueUpdate(CryptoUtils.bytesToBase64(bytes), ts: ts);
          updateValue(update);
        }
      }
    } catch (e) {}
  }

  int subs = 0;

  @override
  Map save() {
    var m = super.save();
    m.remove("?value");
    m.remove("remove");
    m.remove("readBinaryData");
    return m;
  }

  @override
  onRemoving() {
    if (sub != null) {
      sub.cancel();
    }

    fileNodes.remove(this);
  }

  bool needsToWrite = false;

  @override
  onSetValue(Object val) {
    if (isBinary) {
      if (val is String) {
        val = const Utf8Encoder().convert(val);
        data = val;
        needsToWrite = true;
        return true;
      }

      if (val is! ByteData) {
        return true;
      }
    } else {
      if (val is ByteData) {
        try {
          val = const Utf8Decoder().convert(
              (val as ByteData).buffer.asUint8List()
          );
          data = val;
          needsToWrite = true;
        } catch (e) {}
        return true;
      }

      if (val is! String) {
        val = val.toString();
        data = val;
        needsToWrite = true;
        return true;
      }
    }

    needsToWrite = true;
    data = val;
    return true;
  }

  dynamic data;
  FileWatcher watcher;
}

class GroupNode extends SimpleNode {
  GroupNode(String path) : super(path);

  @override
  onCreated() {
    gp(a) {
      var ep = path + "/" + a;
      if (ep.startsWith("//")) ep.substring(1);
      return ep;
    }
    link.addNode(gp("addFile"), ADD_FILE_ACTION);
    link.addNode(gp("addGroup"), ADD_GROUP_ACTION);
    link.addNode(gp("remove"), REMOVE_ACTION);
    link.addNode(gp("addHttpUrl"), ADD_HTTP_URL_ACTION);

    updateList(r"$is");
  }

  @override
  Map save() {
    var m = super.save();
    m.remove("addFile");
    m.remove("addHttpUrl");
    m.remove("addGroup");
    m.remove("remove");
    return m;
  }
}

class AddFileNode extends SimpleNode {
  AddFileNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    var rn = params["name"];
    var p = new Path(path);
    var fp = params["filePath"];
    var isBinary = params["binary"];
    if (rn == null || rn is! String || rn.isEmpty) {
      return {
        "success": false,
        "message": "Name not specified."
      };
    }

    if (isBinary is! bool) {
      isBinary = false;
    }

    if (fp == null || fp is! String || fp.isEmpty) {
      return {
        "success": false,
        "message": "File path not specified."
      };
    }

    var tname = "${p.parentPath}/${Uri.encodeComponent(rn)}";
    if (tname.startsWith("//")) tname = tname.substring(1);
    var node = link.provider.getNode(tname);
    if (node != null && node.disconnected == null) {
      return {
        "success": false,
        "message": "Entity with name '${tname}' already exists."
      };
    }

    SimpleNode n = link.addNode(tname, {
      r"$is": "file",
      r"$name": rn,
      "@filePath": fp,
      "@fileBinary": isBinary
    });

    if (!n.children.containsKey("remove")) {
      n.onCreated();
    }

    link.save();

    return {
      "success": true,
      "message": "Success."
    };
  }
}

class AddHttpUrlNode extends SimpleNode {
  AddHttpUrlNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    var rn = params["name"];
    var p = new Path(path);
    var hu = params["httpUrl"];
    var pr = params["pollRate"];

    if (pr == null) {
      pr = 5000;
    }

    if (pr is! num) {
      return {
        "success": false,
        "message": "Invalid Poll Rate."
      };
    }

    var isBinary = params["binary"];
    if (rn == null || rn is! String || rn.isEmpty) {
      return {
        "success": false,
        "message": "Name not specified."
      };
    }

    if (isBinary is! bool) {
      isBinary = false;
    }

    if (hu == null || hu is! String || hu.isEmpty) {
      return {
        "success": false,
        "message": "Http Url not specified."
      };
    }

    var tname = "${p.parentPath}/${Uri.encodeComponent(rn)}";
    if (tname.startsWith("//")) tname = tname.substring(1);
    var node = link.provider.getNode(tname);
    if (node != null && node.disconnected == null) {
      return {
        "success": false,
        "message": "Entity with name '${tname}' already exists."
      };
    }

    SimpleNode n = link.addNode(tname, {
      r"$is": "http",
      r"$name": rn,
      "@httpUrl": hu,
      "@httpBinary": isBinary,
      "@httpPollRate": pr
    });

    if (!n.children.containsKey("remove")) {
      n.onCreated();
    }

    link.save();

    return {
      "success": true,
      "message": "Success."
    };
  }
}

class AddGroupNode extends SimpleNode {
  AddGroupNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    var p = new Path(path);
    var pp = p.parentPath;
    var name = params["name"];

    if (name == null) {
      throw new Exception("name does not exist.");
    }

    var ep = "${pp}/${Uri.encodeComponent(name)}";
    if (ep.startsWith("//")) ep = ep.substring(1);

    if (link.getNode(ep) != null) {
      return throw new Exception("Entity with name '${name}' already exists.");
    }

    SimpleNode n = link.addNode(ep, {
      r"$is": "group",
      r"$name": name
    });

    if (!n.children.containsKey("addFile")) {
      n.onCreated();
    }

    link.save();
  }
}

class HttpNode extends SimpleNode {
  HttpNode(String path) : super(path);

  bool isBinary = false;
  String url;

  @override
  onCreated() {
    gp(a) {
      var ep = path + "/" + a;
      if (ep.startsWith("//")) ep = ep.substring(1);
      return ep;
    }

    url = attributes["@httpUrl"];
    var rate = attributes[r"@httpPollRate"];

    if (rate == null) {
      rate = 1;
    }

    if (rate is num && rate is! int) {
      rate = rate.toInt();
    }

    if (url == null || rate is! int) {
      link.removeNode(path);
      return;
    }

    pollRate = new Duration(milliseconds: (rate * 1000).toInt());

    if (attributes["@httpBinary"] == true) {
      isBinary = true;
    }

    configs[r"$type"] = isBinary ? "binary" : "string";

    link.addNode(gp("remove"), REMOVE_ACTION);

    updateList(r"$is");

    if (!httpNodes.contains(this)) {
      httpNodes.add(this);
    }
  }

  doUpdate() async {
    if (_isUpdating) {
      return;
    }

    try {
      _isUpdating = true;
      var response = await httpClient.get(url);
      if (response.statusCode != 200) {
        throw new Exception("Status Code: ${response.statusCode}");
      }

      dynamic val;

      if (isBinary) {
        val = response.bodyBytes.buffer.asByteData();
      } else {
        val = response.body;
      }

      var hash = hashData(val);

      if (_lastHash != hash) {
        updateValue(val);
        _lastHash = hash;
      }

      lastUpdate = new DateTime.now();
    } catch (e) {
      updateValue(null);
    }

    _isUpdating = false;
  }

  String _lastHash;

  dynamic hashData(input) {
    var sha = new SHA1();
    if (input is ByteData) {
      sha.add(input.buffer.asUint8List());
    } else {
      sha.add(const Utf8Encoder().convert(input.toString()));
    }
    return CryptoUtils.bytesToHex(sha.close());
  }

  bool _isUpdating = false;

  @override
  onSubscribe() {
    subs++;
  }

  @override
  onUnsubscribe() {
    subs--;
  }

  @override
  onRemoving() {
    clearValue();
    httpNodes.remove(this);
  }

  DateTime lastUpdate = new DateTime.fromMillisecondsSinceEpoch(0);
  Duration pollRate;
  int subs = 0;

  @override
  Map save() {
    var m = super.save();
    m.remove("?value");
    m.remove("remove");
    return m;
  }
}

class ReadyBinaryDataNode extends SimpleNode {
  ReadyBinaryDataNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) {
    Path p = new Path(path);
    FileNode node = provider.getNode(p.parentPath);
    if (node == null || node is! FileNode) {
      throw new Exception("Invalid File Node!");
    }

    return node.file.openRead().map((data) {
      return [
        {
          "chunk": ByteDataUtil.fromUint8List(ByteDataUtil.list2Uint8List(data))
        }
      ];
    });
  }
}

http.Client httpClient = new http.Client();

main(List<String> args) async {
  link = new LinkProvider(args, "File-", profiles: {
    "file": (String path) => new FileNode(path),
    "addFile": (String path) => new AddFileNode(path),
    "remove": (String path) {
      return new DeleteActionNode.forParent(path, link.provider as MutableNodeProvider, onDelete: () {
        link.save();
      });
    },
    "group": (String path) => new GroupNode(path),
    "addGroup": (String path) => new AddGroupNode(path),
    "http": (String path) => new HttpNode(path),
    "addHttpUrl": (String path) => new AddHttpUrlNode(path),
    "readBinaryData": (String path) => new ReadyBinaryDataNode(path)
  }, autoInitialize: false);

  link.init();

  SimpleNode addFileNode = link.addNode("/addFile", ADD_FILE_ACTION);
  addFileNode.serializable = false;

  SimpleNode addHttpNode = link.addNode("/addHttpUrl", ADD_HTTP_URL_ACTION);
  addHttpNode.serializable = false;

  SimpleNode addGroupNode = link.addNode("/addGroup", ADD_GROUP_ACTION);
  addGroupNode.serializable = false;

  var isLooping = false;

  timer = Scheduler.every(Interval.TWO_MILLISECONDS, () async {
    if (isLooping) {
      return;
    }
    isLooping = true;
    var now = new DateTime.now();

    for (var x in httpNodes) {
      var diff = now.millisecondsSinceEpoch - x.lastUpdate.millisecondsSinceEpoch;
      if (x.subs > 0) {
        if (x.lastValueUpdate == null || diff >= x.pollRate.inMilliseconds) {
          x.doUpdate();
        }
      }

      if (x.subs == 0) {
        x.clearValue();
      }
    }

    for (var x in fileNodes) {
      try {
        if (!x.needsToWrite) {
          continue;
        }

        if (x.data is ByteData) {
          x.data = (x.data as ByteData).buffer.asUint8List();
        }

        var val = x.data;
        x.needsToWrite = false;
        x.data = null;

        if (val is String) {
          await x.file.writeAsString(val);
        } else if (val is List) {
          await x.file.writeAsBytes(val);
        }
      } catch (e) {}
    }
    isLooping = false;
  });

  link.connect();
}

Timer timer;

final Map ADD_FILE_ACTION = {
  r"$is": "addFile",
  r"$name": "Add File",
  r"$params": [
    {
      "name": "name",
      "type": "string",
      "description": "File Name"
    },
    {
      "name": "filePath",
      "type": "string",
      "description": "File Path"
    },
    {
      "name": "binary",
      "type": "bool",
      "description": "Load as Binary"
    }
  ],
  r"$result": "values",
  r"$invokable": "write",
  r"$columns": [
    {
      "name": "success",
      "type": "bool"
    },
    {
      "name": "message",
      "type": "string"
    }
  ]
};

final Map ADD_HTTP_URL_ACTION = {
  r"$is": "addHttpUrl",
  r"$name": "Add Http Url",
  r"$params": [
    {
      "name": "name",
      "type": "string",
      "description": "File Name"
    },
    {
      "name": "httpUrl",
      "type": "string",
      "description": "Http Url"
    },
    {
      "name": "pollRate",
      "type": "number",
      "default": 5,
      "description": "Poll Rate in Seconds"
    },
    {
      "name": "binary",
      "type": "bool",
      "description": "Load as Binary"
    }
  ],
  r"$result": "values",
  r"$invokable": "write",
  r"$columns": [
    {
      "name": "success",
      "type": "bool"
    },
    {
      "name": "message",
      "type": "string"
    }
  ]
};

final Map ADD_GROUP_ACTION = {
  r"$is": "addGroup",
  r"$name": "Add Group",
  r"$params": [
    {
      "name": "name",
      "type": "string",
      "description": "File Name"
    }
  ],
  r"$result": "values",
  r"$invokable": "write"
};

final Map REMOVE_ACTION = {
  r"$name": "Remove",
  r"$is": "remove",
  r"$invokable": "write"
};

final Map READ_BINARY_DATA = {
  r"$is": "readBinaryData",
  r"$name": "Read Binary Data",
  r"$params": [],
  r"$result": "stream",
  r"$invokable": "read",
  r"$columns": [
    {
      "name": "chunk",
      "type": "binary"
    }
  ]
};
