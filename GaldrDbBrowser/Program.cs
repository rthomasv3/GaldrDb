using System;
using System.Threading.Tasks;
using Galdr.Native;
using GaldrDbBrowser.Models;
using GaldrDbBrowser.Services;
using SharpWebview.Content;

namespace GaldrDbBrowser;

internal class Program
{
    [STAThread]
    static void Main(string[] args)
    {
#if !DEBUG
        EmbeddedContent embeddedContent = new(embeddedNamespace: "GaldrDbBrowser", contentDir: "FrontEnd.dist");
        Uri embeddedUri = new(embeddedContent.ToWebviewUrl());
#endif

        GaldrBuilder builder = new GaldrBuilder()
            .SetTitle("GaldrDb Browser")
            .SetSize(1280, 800)
            .SetMinSize(800, 600)
#if DEBUG
            .SetDebug(true)
            .SetPort(5173)
            .SetContentProvider(new UrlContent("http://localhost:5173/"))
#else
            .SetPort(embeddedUri.Port)
            .SetContentProvider(embeddedContent)
#endif
            .AddSingleton<DatabaseService>();

        RegisterDatabaseHandlers(builder);

        using Galdr.Native.Galdr galdr = builder
            .Build()
            .Run();
    }

    static void RegisterDatabaseHandlers(GaldrBuilder builder)
    {
        builder.AddFunction("isDatabaseOpen", (DatabaseService db) =>
        {
            return new IsDatabaseOpenResult
            {
                IsOpen = db.IsOpen,
                FilePath = db.FilePath
            };
        });

        builder.AddFunction("browseForDatabase", async (Galdr.Native.Galdr galdr, IDialogService dialogService) =>
        {
            string filePath = "waiting...";

            galdr.Dispatch(() =>
            {
                filePath = dialogService.OpenFileDialog(filterList: "db");
            });

            while (filePath == "waiting...")
            {
                await Task.Delay(50);
            }

            return new BrowseResult
            {
                FilePath = filePath
            };
        });

        builder.AddFunction("openDatabase", (string filePath, DatabaseService db) =>
        {
            return db.OpenDatabase(filePath);
        });

        builder.AddAction("closeDatabase", (DatabaseService db) =>
        {
            db.CloseDatabase();
        });

        builder.AddFunction("getDatabaseStats", (DatabaseService db) =>
        {
            return db.GetDatabaseStats();
        });

        builder.AddFunction("getCollections", (DatabaseService db) =>
        {
            return db.GetCollections();
        });

        builder.AddFunction("getCollectionInfo", (string name, DatabaseService db) =>
        {
            return db.GetCollectionInfo(name);
        });

        builder.AddFunction("queryDocuments", async (QueryRequest request, DatabaseService db) =>
        {
            return await db.QueryDocumentsAsync(request);
        });

        builder.AddFunction("getDocument", async (string collection, int id, DatabaseService db) =>
        {
            return await db.GetDocumentAsync(collection, id);
        });

        builder.AddFunction("insertDocument", async (InsertDocumentRequest request, DatabaseService db) =>
        {
            return await db.InsertDocumentAsync(request);
        });

        builder.AddFunction("replaceDocument", async (ReplaceDocumentRequest request, DatabaseService db) =>
        {
            return await db.ReplaceDocumentAsync(request);
        });

        builder.AddFunction("deleteDocument", async (string collection, int id, DatabaseService db) =>
        {
            return await db.DeleteDocumentAsync(collection, id);
        });
    }
}
