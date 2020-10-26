// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 1.1.
//
// The APL v2.0:
//
//---------------------------------------------------------------------------
//   Copyright (C) 2007-2015 Pivotal Software, Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//---------------------------------------------------------------------------
//
// The MPL v1.1:
//
//---------------------------------------------------------------------------
//  The contents of this file are subject to the Mozilla Public License
//  Version 1.1 (the "License"); you may not use this file except in
//  compliance with the License. You may obtain a copy of the License
//  at http://www.mozilla.org/MPL/
//
//  Software distributed under the License is distributed on an "AS IS"
//  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
//  the License for the specific language governing rights and
//  limitations under the License.
//
//  The Original Code is RabbitMQ.
//
//  The Initial Developer of the Original Code is GoPivotal, Inc.
//  Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
//---------------------------------------------------------------------------

using RabbitMQ.Client;
namespace CoreCode.RabbitMQ.ServiceModel
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.ServiceModel;
    using System.ServiceModel.Channels;
    using System.Xml;
    using System.Text;
   

    internal sealed class RabbitMQOutputChannel : RabbitMQOutputChannelBase
    {
        private RabbitMQTransportBindingElement m_bindingElement;
        private MessageEncoder m_encoder;
        private IModel m_model;

        public RabbitMQOutputChannel(BindingContext context, IModel model, EndpointAddress address)
            : base(context, address)
        {
            m_bindingElement = context.Binding.Elements.Find<RabbitMQTransportBindingElement>();
            MessageEncodingBindingElement encoderElement = context.Binding.Elements.Find<MessageEncodingBindingElement>();
            if (encoderElement != null) {
                m_encoder = encoderElement.CreateMessageEncoderFactory().Encoder;
            }
            m_model = model;
        }

        public override void Send(Message message, TimeSpan timeout)
        {
            if (message.State != MessageState.Closed)
            {
                byte[] body = null;
#if VERBOSE
                DebugHelper.Start();
#endif
                string contents = "";
                using(XmlDictionaryReader rdr = message.GetReaderAtBodyContents())
                {
                    contents = rdr.ReadContentAsString();
                }
                using (MemoryStream str = new MemoryStream())
                {
                    //m_encoder.WriteMessage(message, str);
                    //m_encoder.WriteMessage()
                    body = Encoding.ASCII.GetBytes(contents);//str.ToArray();
                }
                
#if VERBOSE
                DebugHelper.Stop(" #### Message.Send {{\n\tAction={2}, \n\tBytes={1}, \n\tTime={0}ms}}.",
                    body.Length,
                    message.Headers.Action.Remove(0, message.Headers.Action.LastIndexOf('/')));
#endif

                var prop = CreateBasicProperties(message);

                m_model.BasicPublish(base.RemoteAddress.Uri.PathAndQuery.Replace("/",""),
                                   message.Headers.Action,
                                   prop,
                                   body);                
            }
        }

        /// <summary>
        /// Sets Rabbit Message Properties From Context
        /// Added This Method Part of V2
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        private IBasicProperties CreateBasicProperties(Message message)
        {
            try
            {
                var prop = m_model.CreateBasicProperties();

                if(!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "content_type")))
                    prop.ContentType=SetMessagePropertiesFromContext(message, "content_type");

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "content_encoding")))
                    prop.ContentEncoding = SetMessagePropertiesFromContext(message, "content_encoding");

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "priority")))
                    prop.Priority = Convert.ToByte(SetMessagePropertiesFromContext(message, "priority"));

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "correlation_id")))
                    prop.CorrelationId = SetMessagePropertiesFromContext(message, "correlation_id");

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "reply_to")))
                    prop.ReplyTo = SetMessagePropertiesFromContext(message, "reply_to");

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "expiration")))
                    prop.Expiration = SetMessagePropertiesFromContext(message, "expiration");

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "message_id")))
                    prop.MessageId = SetMessagePropertiesFromContext(message, "message_id");

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "timestamp")))
                    prop.Timestamp = new AmqpTimestamp(Convert.ToInt64(SetMessagePropertiesFromContext(message, "timestamp")));

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "type")))
                    prop.Type = SetMessagePropertiesFromContext(message, "type");

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "user_id")))
                    prop.UserId = SetMessagePropertiesFromContext(message, "user_id");

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "app_id")))
                    prop.AppId = SetMessagePropertiesFromContext(message, "app_id");

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "cluster_id")))
                    prop.ClusterId = SetMessagePropertiesFromContext(message, "cluster_id");

                if (!string.IsNullOrEmpty(SetMessagePropertiesFromContext(message, "customheaders")))
                {
                    System.Collections.Generic.Dictionary<string, object> headers = CreateHeaders(SetMessagePropertiesFromContext(message, "customheaders"));
                    
                    if(headers!=null && headers.Count > 0)
                        prop.Headers = headers;
                }

                prop.Persistent = true;

                return prop;
            }
            catch (Exception ex)
            {
                // Ignoring Exceptions
            }
            return null;
        }

        /// <summary>
        /// Reads Message Properties From Context
        /// Added This Method Part of V2
        /// </summary>
        /// <param name="message"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        private string SetMessagePropertiesFromContext(Message message, string propertyName)
        {
            try
            {
                string ns = "https://CoreCode.RabbitMQ.Schemas.PropertySchema";
                
                object output;
                bool success = message.Properties.TryGetValue(ns + "#" + propertyName, out output);
                if(success)
                {
                    return output.ToString();
                }
            }
            catch
            {
                //Ignore Exceptions
            }
            return null;
        }

        /// <summary>
        /// Create Rabbit Message Headers
        /// Added This Method Part of V2
        /// </summary>
        /// <param name="customheaders"></param>
        /// <returns></returns>
        private System.Collections.Generic.Dictionary<string, object> CreateHeaders(string customheaders)
        {
            System.Collections.Generic.Dictionary<string, object> headers = new System.Collections.Generic.Dictionary<string, object>();
            try
            {
                XmlDocument xdoc = new XmlDocument();
                xdoc.LoadXml(customheaders);

                foreach (XmlNode xnode in xdoc.DocumentElement.ChildNodes)
                {
                    string key = xnode.Attributes["key"].Value;
                    string value = xnode.Attributes["value"].Value;

                    if(key.ToLower()!="timestamp")
                        headers.Add(key,value);
                    else
                        headers.Add(key,Convert.ToInt64(value));

                }
            }
            catch
            {
                //ignore exceptions
            }
            return headers;
        }

        public override void Close(TimeSpan timeout)
        {
            if (base.State == CommunicationState.Closed || base.State == CommunicationState.Closing)
                return; // Ignore the call, we're already closing.

            OnClosing();
            OnClosed();
        }

        public override void Open(TimeSpan timeout)
        {
            if (base.State != CommunicationState.Created && base.State != CommunicationState.Closed)
                throw new InvalidOperationException(string.Format("Cannot open the channel from the {0} state.", base.State));

            OnOpening();
            OnOpened();
        }
    }
}
