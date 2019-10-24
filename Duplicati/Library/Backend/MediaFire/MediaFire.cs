using Duplicati.Library.Common.IO;
using Duplicati.Library.Interface;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Duplicati.Library.Backend
{
    // ReSharper disable once UnusedMember.Global
    // This class is instantiated dynamically in the BackendLoader.
    public class Mediafire : IBackend, IStreamingBackend
    {
        private const string AUTHID_OPTION = "authid";

        private readonly string m_accesToken;
        private readonly string m_path;
        private readonly MediafireHelper dbx;

        // ReSharper disable once UnusedMember.Global
        // This constructor is needed by the BackendLoader.
        public Mediafire()
        {
        }

        // ReSharper disable once UnusedMember.Global
        // This constructor is needed by the BackendLoader.
        public Mediafire(string url, Dictionary<string, string> options)
        {
            var uri = new Utility.Uri(url);

            m_path = Library.Utility.Uri.UrlDecode(uri.HostAndPath);
            if (m_path.Length != 0 && !m_path.StartsWith("/", StringComparison.Ordinal))
                m_path = "/" + m_path;

            if (m_path.EndsWith("/", StringComparison.Ordinal))
                m_path = m_path.Substring(0, m_path.Length - 1);

            if (options.ContainsKey(AUTHID_OPTION))
                m_accesToken = options[AUTHID_OPTION];

            dbx = new MediafireHelper(m_accesToken);
        }

        public void Dispose()
        {
            // do nothing
        }

        public string DisplayName
        {
            get { return Strings.Mediafire.DisplayName; }
        }

        public string ProtocolKey
        {
            get { return "mediafire"; }
        }

        private IFileEntry ParseEntry(MetaData md)
        {
            var ife = new FileEntry(md.name);
            if (md.IsFile)
            {
                ife.IsFolder = false;
                ife.Size = (long)md.size;
            }
            else
            {
                ife.IsFolder = true;
            }

            try { ife.LastModification = ife.LastAccess = DateTime.Parse(md.server_modified).ToUniversalTime(); }
            catch { }

            return ife;
        }

        private T HandleListExceptions<T>(Func<T> func)
        {
            try
            {
                return func();
            }
            catch (MediafireException de)
            {
                if (de.errorJSON["error"][".tag"].ToString() == "path" && de.errorJSON["error"]["path"][".tag"].ToString() == "not_found")
                    throw new FolderMissingException();

                throw;
            }
        }

        public IEnumerable<IFileEntry> List()
        {
            var lfr = HandleListExceptions(() => dbx.ListFiles(m_path));
              
            foreach (var md in lfr.entries)
                yield return ParseEntry(md);

            while (lfr.has_more)
            {
                lfr = HandleListExceptions(() => dbx.ListFilesContinue(lfr.cursor));
                foreach (var md in lfr.entries)
                    yield return ParseEntry(md);
            }
        }

        public Task PutAsync(string remotename, string filename, CancellationToken cancelToken)
        {
            using(FileStream fs = File.OpenRead(filename))
                return PutAsync(remotename, fs, cancelToken);
        }

        public void Get(string remotename, string filename)
        {
            using(FileStream fs = File.Create(filename))
                Get(remotename, fs);
        }

        public void Delete(string remotename)
        {
            try
            {
                string path = String.Format("{0}/{1}", m_path, remotename);
                dbx.Delete(path);
            }
            catch (MediafireException)
            {
                // we can catch some events here and convert them to Duplicati exceptions
                throw;
            }
        }

        public IList<ICommandLineArgument> SupportedCommands
        {
            get
            {
                return new List<ICommandLineArgument>(new ICommandLineArgument[] {
                    new CommandLineArgument(AUTHID_OPTION, CommandLineArgument.ArgumentType.Password, Strings.Mediafire.AuthidShort, Strings.Mediafire.AuthidLong(OAuthHelper.OAUTH_LOGIN_URL("mediafire"))),
                });
            }
        }

        public string Description { get { return Strings.Mediafire.Description; } }

        public string[] DNSName
        {
            get { return WebApi.Mediafire.Hosts(); }
        }

        public void Test()
        {
            this.TestList();
        }

        public void CreateFolder()
        {
            try
            {
                dbx.CreateFolder(m_path);
            }
            catch (MediafireException de)
            {

                if (de.errorJSON["error"][".tag"].ToString() == "path" && de.errorJSON["error"]["path"][".tag"].ToString() == "conflict")
                    throw new FolderAreadyExistedException();
                throw;
            }
        }

        public async Task PutAsync(string remotename, Stream stream, CancellationToken cancelToken)
        {
            try
            {
                string path = $"{m_path}/{remotename}";
                await dbx.UploadFileAsync(path, stream, cancelToken);
            }
            catch (MediafireException)
            {
                // we can catch some events here and convert them to Duplicati exceptions
                throw;
            }
        }

        public void Get(string remotename, Stream stream)
        {
            try
            {
                string path = string.Format("{0}/{1}", m_path, remotename);
                dbx.DownloadFile(path, stream);
            }
            catch (MediafireException)
            {
                // we can catch some events here and convert them to Duplicati exceptions
                throw;
            }
        }
    }
}
