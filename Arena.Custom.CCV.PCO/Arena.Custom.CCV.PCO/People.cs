using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Xml;
using System.Xml.Linq;

using Arena.Enums;
using Arena.Core;
using Arena.Utility;

using DotNetOpenAuth.Messaging;
using DotNetOpenAuth.OAuth;
using DotNetOpenAuth.OAuth.ChannelElements;

namespace Arena.Custom.CCV.PCO
{
    public class People
    {
        #region Private Variables

        #region Static Attribute Guids

        // Hard-Coded Attribute Guids so that existence can be reliably tested
        //private static Guid AttributeGroupGuid_PCO = new Guid("C4199FF0-CE15-11DE-A54E-830B56D89593");

        //private static Guid AttributeGuid_PCO_ID = new Guid("71186326-CE16-11DE-9614-531256D89593");
        //private static Guid AttributeGuid_PCO_Password = new Guid("A16A8DD0-D094-11DE-940F-06D555D89593");
        //private static Guid AttributeGuid_PCO_Last_Sync = new Guid("3D8D4C3A-D526-11DE-ADC4-877355D89593");
        //private static Guid AttributeGuid_PCO_Last_Sync_Arena = new Guid("418E514E-D526-11DE-A842-AE7355D89593");

        #endregion

        private WebConsumer _pcoConsumer;
        private int _organizationId;
        private string _accessToken;
        private string _publicArenaUrl;
        private Lookup _pcoAccount;
        private AttributeGroup _pcoAttributeGroup;

        #endregion

        #region Public Properties

        // If a PCO Request returns HTML instead of an XML Response, the HTML
        public string HTMLResponse { get; set; }
        public HttpStatusCode HTTPStatusCode { get; private set; }

        #endregion

        #region Constructors

        public People(int organizationId, Lookup pcoAccount, string publicArenaUrl)
            : this(null, organizationId, pcoAccount, publicArenaUrl)
        {
        }

        public People(IConsumerTokenManager tokenManager, int organizationId, Lookup pcoAccount, string publicArenaUrl)
        {
            IConsumerTokenManager tManager = tokenManager ?? new InMemoryTokenManager(organizationId, pcoAccount);
                
            _organizationId = organizationId;
            _pcoAccount = pcoAccount;
            _pcoAttributeGroup = GetPcoAttributeGroup(_organizationId, _pcoAccount);

            _publicArenaUrl = publicArenaUrl.Trim();
            if (!_publicArenaUrl.EndsWith("/"))
                _publicArenaUrl += "/";

            _accessToken = pcoAccount.Qualifier;
            if (_accessToken.Trim() == string.Empty)
                throw new ApplicationException("'Access Token' does not exist.  Make sure you have successfully authorized your Arena installation with Planning Center Online!");

            _pcoConsumer = new WebConsumer(Consumer.ServiceDescription, tManager);
        }

        #endregion

        #region Public Methods

        public XDocument GetPeople()
        {
            MessageReceivingEndpoint GetPeopleEndpoint =
                        new MessageReceivingEndpoint(
                            "https://www.planningcenteronline.com/people.xml", 
                            HttpDeliveryMethods.GetRequest); 
            
            HttpWebRequest request = _pcoConsumer.PrepareAuthorizedRequest(GetPeopleEndpoint, _accessToken);
            request.Proxy.Credentials = CredentialCache.DefaultCredentials;

            try
            {
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                return GetResponse(response.GetResponseStream(), response.ContentType, response.StatusCode);
            }
            catch (WebException webException)
            {
                SaveResponse(webException.Response.GetResponseStream());
                throw new Exception(webException.Message + " - " + HTMLResponse);
            }
        }

        public XDocument GetPerson(string pcoId)
        {
            if (pcoId != "-1")
            {
                MessageReceivingEndpoint GetPersonEndpoint =
                            new MessageReceivingEndpoint(
                                string.Format("https://www.planningcenteronline.com/people/{0}.xml", pcoId),
                                HttpDeliveryMethods.GetRequest);

                HttpWebRequest request = _pcoConsumer.PrepareAuthorizedRequest(GetPersonEndpoint, _accessToken);
                request.Proxy.Credentials = CredentialCache.DefaultCredentials;

                try
                {
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                    return GetResponse(response.GetResponseStream(), response.ContentType, response.StatusCode);
                }
                catch (WebException webException)
                {
                    if (webException.Response.Headers["Status"].StartsWith("404"))
                    {
                        return null;
                    }
                    else
                    {
                        SaveResponse(webException.Response.GetResponseStream());
                        throw new Exception(webException.Message + " - " + HTMLResponse);
                    }
                }
            }
            else
                return null;
        }

        public XDocument AddPerson(Person person, string userId, bool editor)
        {
            return AddPerson(person, userId, PersonXML(person, _publicArenaUrl, editor));
        }
        
        public XDocument AddPerson(Person person, string userId, XDocument personXML)
        {
            XDocument xResult = SendPCORequest(
                personXML,
                new MessageReceivingEndpoint(
                    "https://www.planningcenteronline.com/people.xml", 
                    HttpDeliveryMethods.PostRequest | HttpDeliveryMethods.AuthorizationHeaderRequest));

            if (HTTPStatusCode == HttpStatusCode.Created)
            {
                var id = from p in xResult.Descendants("person")
                         select (int)p.Element("id");
                if (id.Count() == 1)
                    SavePcoID(_pcoAccount, person, id.First(), userId);

                SetPassword(id.First(), person, userId);
            }

            return xResult;
        }

        public XDocument UpdatePerson(Person person, string userId, bool editor)
        {
            return UpdatePerson(person, null, userId, editor);
        }

        public XDocument UpdatePerson(Person person, XDocument personXML, string userId, bool editor)
        {
            int pcoId = GetPcoID(_pcoAccount, person);
            if (pcoId == -1)
            {
                return AddPerson(person, userId, editor);
            }
            else
            {
                XDocument xResult = SendPCORequest(
                    personXML ?? PersonXML(person, _publicArenaUrl, editor),
                    new MessageReceivingEndpoint(
                        string.Format("https://www.planningcenteronline.com/people/{0}.xml?_method=put", pcoId.ToString()),
                        HttpDeliveryMethods.PutRequest | HttpDeliveryMethods.AuthorizationHeaderRequest));

                if (HTTPStatusCode == HttpStatusCode.OK)
                {
                    SetPassword(pcoId, person, userId);
                }

                return xResult;
            }
        }

        public XDocument SetPassword(int pcoId, Person person, string userId)
        {
            if (pcoId != -1)
            {
                string password = PCOPassword(pcoId);

                XDocument xd = new XDocument(
                    new XDeclaration("1.0", "UTF-8", "yes"),
                    new XElement("person",
                        new XElement("new_password", password),
                        new XElement("new_password_confirmation", password)
                    )
                );

                SavePcoPassword(_pcoAccount, person, password, userId);

                return SendPCORequest(xd,
                    new MessageReceivingEndpoint(
                        string.Format("https://www.planningcenteronline.com/people/{0}.xml?_method=put", pcoId.ToString()),
                        HttpDeliveryMethods.PutRequest | HttpDeliveryMethods.AuthorizationHeaderRequest));
            }
            else
                return null;
        }

        public void Disable(List<int> validUsers)
        {
            XDocument xdocResults = GetPeople();
            if (xdocResults != null)
            {
                AttributeGroup pcoAttrGroup = GetPcoAttributeGroup(_organizationId, _pcoAccount);
                Arena.Core.Attribute pcoIDAttr = GetPcoAttribute(pcoAttrGroup, "PCO_ID", Enums.DataType.String);
                Arena.DataLayer.Organization.OrganizationData oData = new DataLayer.Organization.OrganizationData();

                foreach (XElement xPerson in xdocResults.Descendants("person"))
                {
                    try
                    {
                        string pcoID = xPerson.Descendants("id").First().Value;
                        string sql = string.Format("SELECT person_id FROM core_person_attribute WHERE attribute_id = {0} AND int_value = {1}",
                            pcoIDAttr.AttributeId.ToString(), pcoID);

                        System.Data.SqlClient.SqlDataReader rdr = oData.ExecuteReader(sql);
                        if (rdr.Read())
                        {
                            if (!validUsers.Contains((int)rdr["person_id"]))
                                Disable(pcoID);
                        }
                        rdr.Close();
                    }
                    catch { }
                }
            }
        }

        public void Disable(string pcoId)
        {
            if (pcoId != "-1")
            {
                XDocument xd = new XDocument(
                    new XDeclaration("1.0", "UTF-8", "yes"),
                    new XElement("person",
                        new XElement("permissions", "Disabled")
                    )
                );

                SendPCORequest(xd,
                    new MessageReceivingEndpoint(
                        string.Format("https://www.planningcenteronline.com/people/{0}.xml?_method=put", pcoId),
                        HttpDeliveryMethods.PutRequest | HttpDeliveryMethods.AuthorizationHeaderRequest));
            }
        }

        public HttpStatusCode Login(Person person)
        {
            string postData = string.Format(
                "authenticity_token={0}&email={1}&password={2}", 
                "2hb6ky/922Yn4NdOSjw4YaGMFkZXhPLIvRBpZe6cfzE=",
                person.Emails.FirstActive,
                GetPcoPassword(_pcoAccount, person));
            byte[] data = new ASCIIEncoding().GetBytes(postData);

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create("https://www.planningcenteronline.com/login");
            request.Proxy.Credentials = CredentialCache.DefaultCredentials;
            request.Method = "POST";
            request.ContentType = "application/x-www-form-urlencoded";
            request.ContentLength = data.Length;

            Stream dataStream = request.GetRequestStream();
            dataStream.Write(data, 0, data.Length);
            dataStream.Close();

            HttpWebResponse response = (HttpWebResponse)request.GetResponse();

            Stream receiveStream = response.GetResponseStream();
            Encoding encode = System.Text.Encoding.GetEncoding("utf-8");
            StreamReader readStream = new StreamReader(receiveStream, encode);

            StringBuilder sb = new StringBuilder();
            Char[] read = new Char[256];
            int count = 0;
            do
            {
                count = readStream.Read(read, 0, 256);
                String str = new String(read, 0, count);
                sb.Append(str);
            }
            while (count > 0);

            HTMLResponse = sb.ToString();

            return response.StatusCode;
        }

        #region Sync

        public string SyncPerson(Person person, string userId, bool editor)
        {
            int pcoId = GetPcoID(_pcoAccount, person);

            // Load field names to compare
            Dictionary<string, SyncValues> PersonValues = new Dictionary<string, SyncValues>();
            PersonValues.Add("first-name", new SyncValues());
            PersonValues.Add("last-name", new SyncValues());
            PersonValues.Add("permissions", new SyncValues());
            PersonValues.Add("photo-url", new SyncValues());
            PersonValues.Add("home-address-street", new SyncValues());
            PersonValues.Add("home-address-city", new SyncValues());
            PersonValues.Add("home-address-state", new SyncValues());
            PersonValues.Add("home-address-zip", new SyncValues());
            PersonValues.Add("home-phone", new SyncValues());
            PersonValues.Add("work-phone", new SyncValues());
            PersonValues.Add("mobile-phone", new SyncValues());
            PersonValues.Add("home-email", new SyncValues());

            // Set sync values to compare
            Dictionary<string, string> PCOPrevious = PersonDictionary(GetPcoLastSync(_pcoAccount, person));
            Dictionary<string, string> PCOCurrent = PersonDictionary(GetPerson(pcoId.ToString()));
            Dictionary<string, string> ArenaPrevious = PersonDictionary(GetPcoLastSyncArena(_pcoAccount, person));
            Dictionary<string, string> ArenaCurrent = PersonDictionary(PersonXML(person, _publicArenaUrl, editor));

            // If arena has a PCO ID, but that person does not exist in PCO, then they may have been deleted from PCO.
            // To force an add, pco ID needs to be reset to -1
            if (pcoId != -1 && PCOCurrent.Count == 0)
                SavePcoID(_pcoAccount, person, -1, userId);

            // Object to hold changes that need to be made
            Dictionary<string, string> PCOUpdates = new Dictionary<string, string>();
            Dictionary<string, string> ArenaUpdates = new Dictionary<string, string>();
            Dictionary<string, string> CurrentValues = new Dictionary<string, string>();

            // Compare each field
            foreach (KeyValuePair<string, SyncValues> values in PersonValues)
            {
                // Do not allow image to be synced from PCO
                if (values.Key != "photo-url")
                {
                    if (PCOPrevious.ContainsKey(values.Key))
                        values.Value.PCOPrevious = PCOPrevious[values.Key];

                    if (PCOCurrent.ContainsKey(values.Key))
                        values.Value.PCOCurrent = PCOCurrent[values.Key];
                }

                if (ArenaPrevious.ContainsKey(values.Key))
                    values.Value.ArenaPrevious = ArenaPrevious[values.Key];

                if (ArenaCurrent.ContainsKey(values.Key))
                    values.Value.ArenaCurrent = ArenaCurrent[values.Key];

                values.Value.Compare(values.Key);
                CurrentValues.Add(values.Key, values.Value.Value);

                if (values.Value.PCOUpdateRequired)
                    PCOUpdates.Add(values.Key, values.Value.Value);

                if (values.Value.ArenaUpdateRequired)
                    ArenaUpdates.Add(values.Key, values.Value.Value);
            }

            // Update values 
            if (PCOUpdates.Count > 0 || ArenaUpdates.Count > 0)
            {
                StringBuilder sb = new StringBuilder();
                if (PCOUpdates.Count > 0)
                {
                    UpdatePerson(person, PersonXML(CurrentValues), userId, editor);
                    sb.Append("PCO Record updated!<br/>");
                }

                if (ArenaUpdates.Count > 0)
                {
                    Person personUpdate = UpdatePerson(person, ArenaUpdates);
                    personUpdate.Save(_organizationId, userId, false);
                    personUpdate.SaveEmails(_organizationId, userId);
                    personUpdate.SavePhones(_organizationId, userId);
                    personUpdate.SaveAddresses(_organizationId, userId);

                    sb.Append("Arena Record Updated!<br/>");
                }

                SavePcoLastSync(_pcoAccount, person, GetPerson(GetPcoID(_pcoAccount, person).ToString()), userId);
                SavePcoLastSyncArena(_pcoAccount, person, PersonXML(person, _publicArenaUrl, editor), userId);

                return sb.ToString();
            }
            else
                return "Neither record was updated!";
        }

        #endregion

        #endregion

        #region Private Helper Methods

        private static string PCOPassword(int pcoId)
        {
            char[] AlphaChars = new char[] { 
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

            char[] idChars = pcoId.ToString().ToCharArray();
            Array.Reverse(idChars);

            for (int i = 0; i < idChars.Length - 1; i++)
                idChars[i] = AlphaChars[
                    Int32.Parse(string.Format("{0}{1}", idChars[i], idChars[i + 1])) % AlphaChars.Length];

            return new string(idChars);
        }

        private XDocument SendPCORequest(XDocument data, MessageReceivingEndpoint endpoint)
        {
            string stringData = data.Declaration.ToString() + data.ToString(SaveOptions.DisableFormatting);
            byte[] postData = ASCIIEncoding.ASCII.GetBytes(stringData);

            HttpWebRequest request = _pcoConsumer.PrepareAuthorizedRequest(endpoint, _accessToken);
            request.Proxy.Credentials = CredentialCache.DefaultCredentials;
            request.ContentType = "application/xml";
            request.ContentLength = postData.Length;
            Stream requestStream = request.GetRequestStream();
            requestStream.Write(postData, 0, postData.Length);
            requestStream.Close();

            try
            {
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                return GetResponse(response.GetResponseStream(), response.ContentType, response.StatusCode);
            }
            catch (WebException webException)
            {
                SaveResponse(webException.Response.GetResponseStream());
                throw new Exception(webException.Message + " - " + HTMLResponse);
            }
        }

        private XDocument GetResponse(Stream responseStream, string contentType, HttpStatusCode statusCode)
        {
            HTTPStatusCode = statusCode;

            Stream receiveStream = responseStream;
            Encoding encode = System.Text.Encoding.GetEncoding("utf-8");
            StreamReader readStream = new StreamReader(receiveStream, encode);

            StringBuilder sb = new StringBuilder();
            Char[] read = new Char[8192];
            int count = 0;
            do
            {
                count = readStream.Read(read, 0, 8192);
                String str = new String(read, 0, count);
                sb.Append(str);
            }
            while (count > 0);

            HTMLResponse = sb.ToString();

            if (contentType.ToLower().Contains("xml") &&
                HTMLResponse.Trim().Length > 0)
                return XDocument.Parse(HTMLResponse);
            else
                return null;
        }

        private void SaveResponse(Stream responseStream)
        {
            Stream receiveStream = responseStream;
            Encoding encode = System.Text.Encoding.GetEncoding("utf-8");
            StreamReader readStream = new StreamReader(receiveStream, encode);

            StringBuilder sb = new StringBuilder();
            Char[] read = new Char[8192];
            int count = 0;
            do
            {
                count = readStream.Read(read, 0, 8192);
                String str = new String(read, 0, count);
                sb.Append(str);
            }
            while (count > 0);

            HTMLResponse = sb.ToString();
        }

        #endregion

        #region Static Attribute Methods

        #region PCO ID

        public static int GetPcoID(Lookup pcoAccount, Person person)
        {
            PersonAttribute pa = GetPcoIDAttribute(pcoAccount, person);
            return pa.IntValue;
        }

        public static void SavePcoID(Lookup pcoAccount, Person person, int pcoID, string userId)
        {
            PersonAttribute pa = GetPcoIDAttribute(pcoAccount, person);
            pa.IntValue = pcoID;
            pa.Save(person.OrganizationID, userId);
        }

        private static PersonAttribute GetPcoIDAttribute(Lookup pcoAccount, Person person)
        {
            return GetPcoPersonAttribute(pcoAccount, person, "PCO_ID");
        }

        #endregion

        #region PCO Password

        public static string GetPcoPassword(Lookup pcoAccount, Person person)
        {
            PersonAttribute pa = GetPcoPasswordAttribute(pcoAccount, person);
            return pa.StringValue;
        }

        public static void SavePcoPassword(Lookup pcoAccount, Person person, string password, string userId)
        {
            PersonAttribute pa = GetPcoPasswordAttribute(pcoAccount, person);
            pa.StringValue = password;
            pa.Save(person.OrganizationID, userId);
        }

        private static PersonAttribute GetPcoPasswordAttribute(Lookup pcoAccount, Person person)
        {
            return GetPcoPersonAttribute(pcoAccount, person, "PCO_Password");
        }

        #endregion

        #region PCO Last Sync

        public static XDocument GetPcoLastSync(Lookup pcoAccount, Person person)
        {
            PersonAttribute pa = GetPcoLastSyncAttribute(pcoAccount, person);
            ArenaDataBlob blobDetails = new ArenaDataBlob(pa.IntValue);
            if (blobDetails.ByteArray != null && blobDetails.ByteArray.Length > 0)
            {
                XmlReaderSettings settings = new XmlReaderSettings();
                using (MemoryStream stream = new MemoryStream(blobDetails.ByteArray))
                    using (XmlReader reader = XmlReader.Create(stream, settings))
                    {
                        return XDocument.Load(reader);
                    }
            }
            else
                return null;
        }

        public static void SavePcoLastSync(Lookup pcoAccount, Person person, XDocument details, string userId)
        {
            PersonAttribute pa = GetPcoLastSyncAttribute(pcoAccount, person);
            Arena.Document.PersonDocument personDocument = new Arena.Document.PersonDocument(person.PersonID, pa.IntValue);
            personDocument.PersonID = person.PersonID;

            using (MemoryStream stream = new MemoryStream())
            {
                XmlWriterSettings settings = new XmlWriterSettings();
                using (XmlWriter writer = XmlWriter.Create(stream, settings))
                {
                    details.Save(writer);
                }
                personDocument.ByteArray = stream.ToArray();
            }

            personDocument.OriginalFileName = "PCOPerson.xml";
            personDocument.FileExtension = "xml";
            personDocument.MimeType = "application/xml";
            personDocument.Title = "Last PCO Sync";
            personDocument.Description = string.Format("PCO Attributes for {0}({1})",
                person.FullName, person.PersonGUID.ToString());
            personDocument.Save(userId);

            pa.IntValue = personDocument.DocumentID;
            pa.Save(person.OrganizationID, userId);
        }

        private static PersonAttribute GetPcoLastSyncAttribute(Lookup pcoAccount, Person person)
        {
            return GetPcoPersonAttribute(pcoAccount, person, "PCO_Last_Sync");
        }

        #endregion

        #region PCO Last Sync Arena

        public static XDocument GetPcoLastSyncArena(Lookup pcoAccount, Person person)
        {
            PersonAttribute pa = GetPcoLastSyncArenaAttribute(pcoAccount, person);
            ArenaDataBlob blobDetails = new ArenaDataBlob(pa.IntValue);
            if (blobDetails.ByteArray != null && blobDetails.ByteArray.Length > 0)
            {
                XmlReaderSettings settings = new XmlReaderSettings();
                using (MemoryStream stream = new MemoryStream(blobDetails.ByteArray))
                using (XmlReader reader = XmlReader.Create(stream, settings))
                {
                    return XDocument.Load(reader);
                }
            }
            else
                return null;
        }

        public static void SavePcoLastSyncArena(Lookup pcoAccount, Person person, XDocument details, string userId)
        {
            PersonAttribute pa = GetPcoLastSyncArenaAttribute(pcoAccount, person);
            Arena.Document.PersonDocument personDocument = new Arena.Document.PersonDocument(person.PersonID, pa.IntValue);
            personDocument.PersonID = person.PersonID;

            using (MemoryStream stream = new MemoryStream())
            {
                XmlWriterSettings settings = new XmlWriterSettings();
                using (XmlWriter writer = XmlWriter.Create(stream, settings))
                {
                    details.Save(writer);
                }
                personDocument.ByteArray = stream.ToArray();
            }

            personDocument.OriginalFileName = "ArenaPerson.xml";
            personDocument.FileExtension = "xml";
            personDocument.MimeType = "application/xml";
            personDocument.Title = "Last PCO Sync Arena Data";
            personDocument.Description = string.Format("Arena Attributes for {0}({1})",
                person.FullName, person.PersonGUID.ToString());
            personDocument.Save(userId);

            pa.IntValue = personDocument.DocumentID;
            pa.Save(person.OrganizationID, userId);
        }

        private static PersonAttribute GetPcoLastSyncArenaAttribute(Lookup pcoAccount, Person person)
        {
            return GetPcoPersonAttribute(pcoAccount, person, "PCO_Last_Sync_Arena");
        }

        #endregion

        #region Generic Attribute Methods

        public static AttributeGroup GetPcoAttributeGroup(int organizationId, Lookup pcoAccount)
        {
            if (pcoAccount.Qualifier2.Trim() == string.Empty)
            {
                pcoAccount.Qualifier2 = Guid.NewGuid().ToString();
                pcoAccount.Save();
            }

            Guid groupGuid = new Guid(pcoAccount.Qualifier2);

            AttributeGroup pcoAttributeGroup = new AttributeGroup(groupGuid);

            if (pcoAttributeGroup.AttributeGroupId == -1)
            {
                Arena.DataLayer.Organization.OrganizationData oData = new Arena.DataLayer.Organization.OrganizationData();

                string sql = "SELECT ISNULL(MAX(group_order),-1) FROM core_attribute_group";
                int groupOrder = (int)oData.ExecuteScalar(sql);

                sql = string.Format(
                    "insert into core_attribute_group (guid, organization_id, group_name, group_order, display_location) values ('{0}', {1}, 'PCO - {2}', (({3})+2), 0)",
                    groupGuid.ToString(),
                    organizationId.ToString(),
                    pcoAccount.Value,
                    groupOrder.ToString());

                oData.ExecuteNonQuery(sql);

                pcoAttributeGroup = new AttributeGroup(groupGuid);
            }

            GetPcoAttribute(pcoAttributeGroup, "PCO_Last_Sync", DataType.Document);
            GetPcoAttribute(pcoAttributeGroup, "PCO_Last_Sync_Arena", DataType.Document);
            GetPcoAttribute(pcoAttributeGroup, "PCO_ID", DataType.Int);
            GetPcoAttribute(pcoAttributeGroup, "PCO_Password", DataType.String);

            return pcoAttributeGroup;
        }
        public static Arena.Core.Attribute GetPcoAttribute(AttributeGroup attributeGroup, string attributeName, DataType attributeType)
        {
            Arena.Core.Attribute pcoAttribute = attributeGroup.Attributes.FindByName(attributeName);

            if (pcoAttribute == null || pcoAttribute.AttributeId == -1)
            {
                Arena.DataLayer.Organization.OrganizationData oData = new Arena.DataLayer.Organization.OrganizationData();

                string sql = string.Format(
                    "SELECT ISNULL(MAX(attribute_order),-1) FROM core_attribute WHERE attribute_group_id = {0}",
                    attributeGroup.AttributeGroupId.ToString());
                int groupOrder = (int)oData.ExecuteScalar(sql);

                Guid attributeGuid = Guid.NewGuid();

                sql = string.Format(
                    "insert into core_attribute (guid, attribute_group_id, attribute_name, attribute_type, attribute_order) values ('{0}', {1}, '{2}', '{3}', (({4})+2))",
                    attributeGuid.ToString(),
                    attributeGroup.AttributeGroupId.ToString(),
                    attributeName,
                    Enum.Format(typeof(DataType), attributeType, "D"),
                    groupOrder.ToString());

                oData.ExecuteNonQuery(sql);

                pcoAttribute = new Arena.Core.Attribute(attributeGuid);
                attributeGroup.Attributes.Add(pcoAttribute);
            }
            
            return pcoAttribute;
        }

        private static PersonAttribute GetPcoPersonAttribute(Lookup pcoAccount, Person person, string attributeName)
        {
            AttributeGroup attributeGroup = GetPcoAttributeGroup(person.OrganizationID, pcoAccount);
            if (attributeGroup != null)
            {
                Arena.Core.Attribute pcoAttribute = attributeGroup.Attributes.FindByName(attributeName);
                if (pcoAttribute != null)
                {
                    PersonAttribute pa = (PersonAttribute)person.Attributes.FindByID(pcoAttribute.AttributeId);
                    if (pa == null)
                        pa = new PersonAttribute(person.PersonID, pcoAttribute.AttributeId);
                    return pa;
                }
            }
            return null;
        }

        #endregion

        #endregion

        #region Xml Dictionary Conversion Methods

        public static Person UpdatePerson(Person person, Dictionary<string, string> updates)
        {
            Person updatedPerson = person;

            foreach (KeyValuePair<string, string> update in updates)
            {
                switch (update.Key)
                {
                    case "first-name":
                        updatedPerson.NickName = update.Value ?? "";
                        break;
                    case "last-name":
                        updatedPerson.LastName = update.Value ?? "";
                        break;

                    case "home-address-street":
                        PersonAddress(updatedPerson, SystemLookup.AddressType_Home).Address.StreetLine1 = update.Value ?? "";
                        break;
                    case "home-address-city":
                        PersonAddress(updatedPerson, SystemLookup.AddressType_Home).Address.City = update.Value ?? "";
                        break;
                    case "home-address-state":
                        PersonAddress(updatedPerson, SystemLookup.AddressType_Home).Address.State = update.Value ?? "";
                        break;
                    case "home-address-zip":
                        PersonAddress(updatedPerson, SystemLookup.AddressType_Home).Address.PostalCode = update.Value ?? "";
                        break;

                    case "home-phone":
                        PersonPhone(updatedPerson, SystemLookup.PhoneType_Home).Number = update.Value ?? "";
                        break;
                    case "work-phone":
                        PersonPhone(updatedPerson, SystemLookup.PhoneType_Business).Number = update.Value ?? "";
                        break;
                    case "mobile-phone":
                        PersonPhone(updatedPerson, SystemLookup.PhoneType_Cell).Number = update.Value ?? "";
                        break;

                    case "home-email":
                        updatedPerson.Emails.FirstActive = update.Value ?? "";
                        break;
                }
            }

            return updatedPerson;
        }

        private static PersonAddress PersonAddress(Person person, Guid addressType)
        {
            PersonAddress personAddress = person.Addresses.FindByType(addressType);
            if (personAddress == null)
            {
                personAddress = new PersonAddress();
                personAddress.AddressType = new Lookup(addressType);
                personAddress.Address = new Address();
                person.Addresses.Add(personAddress);
            }
            return personAddress;
        }

        private static PersonPhone PersonPhone(Person person, Guid phoneType)
        {
            PersonPhone personPhone = person.Phones.FindByType(phoneType);
            if (personPhone == null)
            {
                personPhone = new PersonPhone();
                personPhone.PhoneType = new Lookup(phoneType);
                person.Phones.Add(personPhone);
            }
            return personPhone;
        }

        public static Dictionary<string, string> PersonDictionary(XDocument person)
        {
            Dictionary<string, string> dict = new Dictionary<string, string>();

            if (person != null)
            {
                // First Name
                XElement xFirstName = person.Root.Element("first-name");
                if (xFirstName != null)
                    dict.Add("first-name", xFirstName.Value);

                // Last Name
                XElement xLastName = person.Root.Element("last-name");
                if (xLastName != null)
                    dict.Add("last-name", xLastName.Value);

                // Permissions
                XElement xPermissions = person.Root.Element("permissions");
                if (xPermissions != null)
                    dict.Add("permissions", xPermissions.Value);

                // Photo
                XElement xPhotoUrl = person.Root.Element("photo-url");
                if (xPhotoUrl != null)
                    dict.Add("photo-url", xPhotoUrl.Value);

                // Contact Information
                XElement xContactData = person.Root.Element("contact_data");
                if (xContactData != null)
                {
                    // Address
                    XElement xAddresses = xContactData.Element("addresses");
                    if (xAddresses != null)
                    {
                        XElement xHomeAddress = locationElement(xAddresses, "address", "Home");
                        if (xHomeAddress != null)
                        {
                            dict.Add("home-address-street", xHomeAddress.Element("street").Value);
                            dict.Add("home-address-city", xHomeAddress.Element("city").Value);
                            dict.Add("home-address-state", xHomeAddress.Element("state").Value);
                            dict.Add("home-address-zip", xHomeAddress.Element("zip").Value);
                        }
                    }

                    // Phone
                    XElement xPhones = xContactData.Element("phone-numbers");
                    if (xPhones != null)
                    {
                        XElement xHomePhone = locationElement(xPhones, "phone-number", "Home");
                        if (xHomePhone != null)
                            dict.Add("home-phone", xHomePhone.Element("number").Value);

                        XElement xWorkPhone = locationElement(xPhones, "phone-number", "Work");
                        if (xWorkPhone != null)
                            dict.Add("work-phone", xWorkPhone.Element("number").Value);

                        XElement xMobilePhone = locationElement(xPhones, "phone-number", "Mobile");
                        if (xMobilePhone != null)
                            dict.Add("mobile-phone", xMobilePhone.Element("number").Value);
                    }

                    // Email
                    XElement xEmails = xContactData.Element("email-addresses");
                    if (xEmails != null)
                    {
                        XElement xHomeEmail = locationElement(xEmails, "email-address", "Home");
                        if (xHomeEmail != null)
                            dict.Add("home-email", xHomeEmail.Element("address").Value);
                    }
                }
            }

            return dict;
        }

        private static XElement locationElement(XElement parentElement, string nodeName, string location)
        {
            var locationNodes = from el in parentElement.Descendants(nodeName)
                                where el.Element("location").Value == location
                                select el;
            if (locationNodes.Count() > 0)
                return locationNodes.First();
            else
                return null;
        }

        public static XDocument PersonXML(Dictionary<string, string> personDict)
        {
            XElement personElement = new XElement("person");

            if (personDict.ContainsKey("first-name"))
                personElement.Add(new XElement("first-name", personDict["first-name"]));

            if (personDict.ContainsKey("last-name"))
                personElement.Add(new XElement("last-name", personDict["last-name"]));

            if (personDict.ContainsKey("permissions"))
                personElement.Add(new XElement("permissions", personDict["permissions"]));

            if (personDict.ContainsKey("photo-url") && personDict["photo-url"] != string.Empty)
                personElement.Add(new XElement("photo-url", personDict["photo-url"]));

            XElement contactElement = new XElement("contact_data");
            personElement.Add(contactElement);

            XElement addressesElement = new XElement("addresses", new XAttribute("type", "array"));
            contactElement.Add(addressesElement);

            if (personDict.ContainsKey("home-address-street"))
                addressesElement.Add(
                    new XElement("address",
                        new XElement("street", personDict["home-address-street"] ?? ""),
                        new XElement("city", personDict["home-address-city"] ?? ""),
                        new XElement("state", personDict["home-address-state"] ?? ""),
                        new XElement("zip", personDict["home-address-zip"] ?? ""),
                        new XElement("location", "Home")
                    )
                );

            XElement phonesElement = new XElement("phone-numbers", new XAttribute("type", "array"));
            contactElement.Add(phonesElement);

            if (personDict.ContainsKey("home-phone") && personDict["home-phone"] != string.Empty)
                phonesElement.Add(
                    new XElement("phone-number",
                        new XElement("number", personDict["home-phone"]),
                        new XElement("location", "Home")
                    )
                );

            if (personDict.ContainsKey("work-phone") && personDict["work-phone"] != string.Empty)
                phonesElement.Add(
                    new XElement("phone-number",
                        new XElement("number", personDict["work-phone"]),
                        new XElement("location", "Work")
                    )
                );

            if (personDict.ContainsKey("mobile-phone") && personDict["mobile-phone"] != string.Empty)
                phonesElement.Add(
                    new XElement("phone-number",
                        new XElement("number", personDict["mobile-phone"]),
                        new XElement("location", "Mobile")
                    )
                );

            XElement emailAddresses = new XElement("email-addresses", new XAttribute("type", "array"));
            contactElement.Add(emailAddresses);

            if (personDict.ContainsKey("home-email") && personDict["home-email"] != string.Empty)
                emailAddresses.Add(
                    new XElement("email-address",
                        new XElement("address", personDict["home-email"]),
                        new XElement("location", "Home")
                    )
                );

            return new XDocument(new XDeclaration("1.0", "UTF-8", "yes"), personElement);
        }

        public static XDocument PersonXML(Person person, string publicArenaUrl, bool editor)
        {
            XElement personElement =
                new XElement("person",
                    new XElement("first-name", person.NickName),
                    new XElement("last-name", person.LastName),
                    new XElement("permissions", editor ? "Editor" : "Scheduled Viewer")
                );

            if ((publicArenaUrl.Trim() != "") && person.HasPhoto)
            {
                personElement.Add(
                    new XElement("photo-url",
                        string.Format("{0}CachedBlob.aspx?guid={1}&width=600&height=600&updated={2}",
                            publicArenaUrl,
                            person.Blob.GUID.ToString(),
                            person.Blob.DateModified.ToString("yyyyMMdd-HHmmss"))
                    )
                );
            }

            XElement contactElement = new XElement("contact_data");
            personElement.Add(contactElement);

            XElement addressesElement = new XElement("addresses", new XAttribute("type", "array"));
            contactElement.Add(addressesElement);

            PersonAddress homeAddress = person.Addresses.FindByType(SystemLookup.AddressType_Home);
            if (homeAddress != null)
                addressesElement.Add(
                    new XElement("address",
                        new XElement("street", homeAddress.Address.StreetLine1),
                        new XElement("city", homeAddress.Address.City),
                        new XElement("state", homeAddress.Address.State),
                        new XElement("zip", homeAddress.Address.PostalCode),
                        new XElement("location", "Home")
                    )
                );

            XElement phonesElement = new XElement("phone-numbers", new XAttribute("type", "array"));
            contactElement.Add(phonesElement);

            XElement homePhoneElement = PhoneElement(person.Phones.FindByType(SystemLookup.PhoneType_Home), "Home");
            if (homePhoneElement != null) phonesElement.Add(homePhoneElement);
            XElement workPhoneElement = PhoneElement(person.Phones.FindByType(SystemLookup.PhoneType_Business), "Work");
            if (workPhoneElement != null) phonesElement.Add(workPhoneElement);
            XElement mobilePhoneElement = PhoneElement(person.Phones.FindByType(SystemLookup.PhoneType_Cell), "Mobile");
            if (mobilePhoneElement != null) phonesElement.Add(mobilePhoneElement);

            XElement emailAddresses = new XElement("email-addresses", new XAttribute("type", "array"));
            contactElement.Add(emailAddresses);

            if (person.Emails.FirstActive != string.Empty)
                emailAddresses.Add(
                    new XElement("email-address",
                        new XElement("address", person.Emails.FirstActive),
                        new XElement("location", "Home")
                    )
                );

            return new XDocument(new XDeclaration("1.0", "UTF-8", "yes"), personElement);
        }

        private static XElement PhoneElement(PersonPhone personPhone, string location)
        {
            XElement phoneElement = null;

            if (personPhone != null &&
                !personPhone.Unlisted &&
                personPhone.Number.Trim() != string.Empty)
            {
                phoneElement =
                    new XElement("phone-number",
                        new XElement("number", personPhone.Number),
                        new XElement("location", location)
                    );
            }

            return phoneElement;
        }

        #endregion
    }

    #region SyncValues Classes

    internal class SyncValues
    {
        public string PCOPrevious { get; set; }
        public string PCOCurrent { get; set; }
        public string ArenaPrevious { get; set; }
        public string ArenaCurrent { get; set; }
        public string Value { get; set; }
        public bool ArenaUpdateRequired { get; set; }
        public bool PCOUpdateRequired { get; set; }


        public SyncValues()
        {
            ArenaUpdateRequired = false;
            PCOUpdateRequired = false;
        }

        public void Compare(string key)
        {
            PCOPrevious = PCOPrevious ?? string.Empty;
            PCOCurrent = PCOCurrent ?? string.Empty;
            ArenaPrevious = ArenaPrevious ?? string.Empty;
            ArenaCurrent = ArenaCurrent ?? string.Empty;

            if (ArenaCurrent != ArenaPrevious)
            {
                // Arena was updated
                Value = ArenaCurrent;
                if (PCOCurrent != ArenaCurrent)
                    // Update PCO
                    PCOUpdateRequired = true;
            }
            else
            {
                if (PCOCurrent != PCOPrevious)
                {
                    // Arena was not updated and PCO was
                    if (ArenaCurrent != PCOCurrent)
                    {
                        if (PCOCurrent.Trim() == string.Empty)
                        {
                            // PCO value was blanked out, but Arena still has value.  Update PCO back to current Arena value
                            Value = ArenaCurrent;
                            PCOUpdateRequired = true;
                        }
                        else
                        {
                            // PCO value was updated to a new non-blank value.  Update Arena
                            Value = PCOCurrent;
                            ArenaUpdateRequired = true;
                        }
                    }
                    else
                        // Arena already equals new PCO Value, thus no update required
                        Value = PCOCurrent;
                }
                else
                {
                    Value = ArenaCurrent;
                    if (PCOCurrent != ArenaCurrent)
                    {
                        // Neither systems were updated, but current values are different
                        if (key != "photo-url")
                            // As long as it's not the photo, update the PCO value to reflect Arena
                            PCOUpdateRequired = true;
                    }
                }
            }

            // Do not downgrade the pco permissions
            if (key.ToLower() == "permissions" && PCOUpdateRequired)
            {
                int oldIndex = 0;
                int newIndex = 0;
                string[] permissions = { "disabled", "scheduled viewer", "viewer", "scheduler", "editor", "administrator" };
                for (int i = 0; i < permissions.Length; i++)
                {
                    if (permissions[i] == PCOCurrent.ToLower())
                        oldIndex = i;
                    if (permissions[i] == Value.ToLower())
                        newIndex = i;
                }

                if (oldIndex > newIndex)
                    PCOUpdateRequired = false;
            }
        }
    }

    #endregion

}
