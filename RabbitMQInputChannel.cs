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

using System;
using System.Diagnostics;
using System.IO;
using System.ServiceModel;
using System.ServiceModel.Channels;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing;
using System.Text;
using System.Collections.Generic;
using System.Xml;

namespace CoreCode.RabbitMQ.ServiceModel
{
    internal sealed class RabbitMQInputChannel : RabbitMQInputChannelBase
    {
        private RabbitMQTransportBindingElement m_bindingElement;
        private MessageEncoder m_encoder;
        private IModel m_model;
        private QueueingBasicConsumer m_messageQueue;
        private const string PropertiesToPromoteKey = "http://schemas.microsoft.com/BizTalk/2006/01/Adapters/WCF-properties/Promote";
        private const string PropertiesToWriteKey = "http://schemas.microsoft.com/BizTalk/2006/01/Adapters/WCF-properties/WriteToContext";
        private const string EsbServiceContextNamespace = "http://schemas.CoreCode.net/integration/enterpriseservices/common/sharedschemas/clinicalproperties";
        private const string PropertiesToPromote = "PropertiesToPromote";
        private const string PropertiesToWrite = "PropertiesToWrite";

        public RabbitMQInputChannel(BindingContext context, IModel model, EndpointAddress address)
            : base(context, address)
        {
            m_bindingElement = context.Binding.Elements.Find<RabbitMQTransportBindingElement>();
            TextMessageEncodingBindingElement encoderElem = context.BindingParameters.Find<TextMessageEncodingBindingElement>();
            encoderElem.ReaderQuotas.MaxStringContentLength = (int)m_bindingElement.MaxReceivedMessageSize;
            if (encoderElem != null) {
                m_encoder = encoderElem.CreateMessageEncoderFactory().Encoder;
            }
            m_model = model;
            m_messageQueue = null;
        }
        /// <summary>
        /// Adds a new property to the specified list.
        /// </summary>
        /// <param name="name">The name of a property.</param>
        /// <param name="ns">The namespace of a property.</param>
        /// <param name="value">The value of a property.</param>
        /// <param name="listName">The name of the list.</param>
        /// <param name="list">The list to add the property to.</param>
        private void AddItemToList(string name,
                                   string ns,
                                   object value,
                                   string listName,
                                   List<KeyValuePair<XmlQualifiedName, object>> list)
        {
            list.Add(new KeyValuePair<XmlQualifiedName, object>(new XmlQualifiedName(name, ns), value));

        }

        public override Message Receive(TimeSpan timeout)
        {
            try
            {
                BasicDeliverEventArgs msg=null;
                try
                {
                    msg = m_messageQueue.Queue.Dequeue() as BasicDeliverEventArgs;
                }
                catch   // Catching Connection Exceptions
                {
                    // Adding Code to Create New Connection on Lost RabbitMQ Connection
                    // Creates New Connection only if CommunicationState is Opened/Created
                    // CommunicationState.Opened = State is Set as Opened when RabbitMQ connection is established
                    // CommunicationState.Created = Method "Open" is called to Open RabbitMQ Connection, In Method Open when State is not Created/Closed it throws exception. 
                    // CommunicationState.Closed = It is when Receive Port is disabled/ host is restarted.
                    if (base.State == CommunicationState.Opened || base.State == CommunicationState.Created)    
                    {
                        System.Diagnostics.EventLog.WriteEntry("BizTalk Server",
                            "RabbitMQ Connection lost for end point " + base.LocalAddress.Uri + ". Reestablishing connection",
                            EventLogEntryType.Error);
                        ChangeStateToCreated();
                        m_model = m_bindingElement.Open(timeout);
                        Open(timeout);
                        msg = m_messageQueue.Queue.Dequeue() as BasicDeliverEventArgs;

                        System.Diagnostics.EventLog.WriteEntry("BizTalk Server",
                            "RabbitMQ Connection Successfully Established " + base.LocalAddress.Uri,
                            EventLogEntryType.Information);
                    }
                    else
                    {
                        // Closing Existing Connection in RabbitMQ
                        Close();
                        return null;
                    }
                }
#if VERBOSE
                DebugHelper.Start();
#endif
                var ms = new MemoryStream(msg.Body);
                var bodyString = Encoding.UTF8.GetString(ms.ToArray());
                if (msg.RoutingKey.Contains(".HL7.") || msg.RoutingKey.Contains(".Xml."))
                bodyString = "<![CDATA[" + bodyString.Trim() + "]]>";

                System.Diagnostics.Debug.Write(bodyString);
                bodyString = String.Concat(@"<s:Envelope xmlns:s=""http://www.w3.org/2003/05/soap-envelope"" xmlns:a=""http://www.w3.org/2005/08/addressing""><s:Header><a:MessageID>urn:", Guid.NewGuid().ToString(), "</a:MessageID></s:Header><s:Body>", System.Web.HttpUtility.HtmlEncode(bodyString), "</s:Body></s:Envelope>");
 
                ms = new MemoryStream(Encoding.UTF8.GetBytes(bodyString));
 
                Message result = m_encoder.ReadMessage(ms, (int)m_bindingElement.MaxReceivedMessageSize);
              
                
               
                var propertiesToPromoteList = new List<KeyValuePair<XmlQualifiedName, object>>();
                var propertiesToWriteList = new List<KeyValuePair<XmlQualifiedName, object>>();
                if (msg.BasicProperties.Headers != null)
                {
                    foreach (var property in msg.BasicProperties.Headers)
                    {
                        try
                        {
                            if (property.Value.GetType() == typeof(Byte[]))
                            {
                                AddItemToList(property.Key, EsbServiceContextNamespace, System.Text.Encoding.UTF8.GetString((Byte[])property.Value), PropertiesToPromote, propertiesToWriteList);
                            }
                            else if (property.Value.GetType() == typeof(Int64) || property.Value.GetType() == typeof(bool))
                            {
                                AddItemToList(property.Key, EsbServiceContextNamespace, property.Value.ToString(), PropertiesToPromote, propertiesToWriteList);
                            }

                        }
                        catch { }
                    }
                }

                ReadMessageProperties(msg, PropertiesToPromote, propertiesToWriteList);

                if (propertiesToPromoteList.Count>0)
                    result.Properties.Add(PropertiesToPromoteKey, propertiesToPromoteList);

                if (propertiesToWriteList.Count > 0)
                    result.Properties.Add(PropertiesToWriteKey, propertiesToWriteList);
               // Message result = m_encoder.ReadMessage(new MemoryStream(msg.Body), (int)m_bindingElement.MaxReceivedMessageSize);

                

                result.Headers.To = base.LocalAddress.Uri;

                m_messageQueue.Model.BasicAck(msg.DeliveryTag, false);
#if VERBOSE
                DebugHelper.Stop(" #### Message.Receive {{\n\tAction={2}, \n\tBytes={1}, \n\tTime={0}ms}}.",
                        msg.Body.Length,
                        result.Headers.Action.Remove(0, result.Headers.Action.LastIndexOf('/')));
#endif         
                return result;
            }
            catch (EndOfStreamException)
            {
                if (m_messageQueue== null || m_messageQueue.ShutdownReason != null && m_messageQueue.ShutdownReason.ReplyCode != Constants.ReplySuccess)
                {
                    OnFaulted();
                }
                Close();
                return null;
            }
        }

        /// <summary>
        /// Reads Message Properties & writes to context
        /// Added This Method Part of V2
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="PropertiesToPromote"></param>
        /// <param name="propertiesToWriteList"></param>
        private void ReadMessageProperties(BasicDeliverEventArgs msg, string PropertiesToPromote, List<KeyValuePair<XmlQualifiedName, object>> propertiesToWriteList)
        {
            string ns = "https://CoreCode.RabbitMQ.Schemas.PropertySchema";
            ReadMessageProperties("content_type", ns, msg.BasicProperties.ContentType, propertiesToWriteList);
            ReadMessageProperties("content_encoding", ns, msg.BasicProperties.ContentEncoding, propertiesToWriteList);
            ReadMessageProperties("priority", ns, msg.BasicProperties.Priority, propertiesToWriteList);
            ReadMessageProperties("correlation_id", ns, msg.BasicProperties.CorrelationId, propertiesToWriteList);
            ReadMessageProperties("reply_to", ns, msg.BasicProperties.ReplyTo, propertiesToWriteList);
            ReadMessageProperties("expiration", ns, msg.BasicProperties.Expiration, propertiesToWriteList);
            ReadMessageProperties("message_id", ns, msg.BasicProperties.MessageId, propertiesToWriteList);
            ReadMessageProperties("timestamp", ns, msg.BasicProperties.Timestamp, propertiesToWriteList);
            ReadMessageProperties("type", ns, msg.BasicProperties.Type, propertiesToWriteList);
            ReadMessageProperties("user_id", ns, msg.BasicProperties.UserId, propertiesToWriteList);
            ReadMessageProperties("app_id", ns, msg.BasicProperties.AppId, propertiesToWriteList);
            ReadMessageProperties("cluster_id", ns, msg.BasicProperties.ClusterId, propertiesToWriteList);
        }


        /// <summary>
        /// Generic Method Which Adds to propertiesToWriteList based on Object type
        /// Added This Method Part of V2
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="ns"></param>
        /// <param name="value"></param>
        /// <param name="propertiesToWriteList"></param>
        private void ReadMessageProperties<T>(string key, string ns, T value, List<KeyValuePair<XmlQualifiedName, object>> propertiesToWriteList)
        {
            try
            {
                if (typeof(T) == typeof(string) && !string.IsNullOrEmpty(value.ToString()))
                {
                    AddItemToList(key, ns, value, PropertiesToPromote, propertiesToWriteList);
                }
                else if (typeof(T) == typeof(Byte) && Convert.ToByte(value) != 0)
                {
                    AddItemToList(key, ns, value, PropertiesToPromote, propertiesToWriteList);
                }
                else if (typeof(T) == typeof(Int64) && Convert.ToInt64(value) != 0 )
                {
                    AddItemToList(key, ns, value, PropertiesToPromote, propertiesToWriteList);
                }
            }
            catch
            {
                // Ignore Exceptions
            }
        }

        public override bool TryReceive(TimeSpan timeout, out Message message)
        {
            message = Receive(timeout);
            return true;
        }

        public override bool WaitForMessage(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public override void Close(TimeSpan timeout)
        {

            if (base.State == CommunicationState.Closed
                || base.State == CommunicationState.Closing)
            {
                return; // Ignore the call, we're already closing.
            }

            OnClosing();
#if VERBOSE
            DebugHelper.Start();
#endif
            if (m_messageQueue != null) {
                m_model.BasicCancel(m_messageQueue.ConsumerTag);
                m_messageQueue = null;
            }
#if VERBOSE
            DebugHelper.Stop(" ## In.Channel.Close {{\n\tAddress={1}, \n\tTime={0}ms}}.", LocalAddress.Uri.PathAndQuery);
#endif
            OnClosed();
        }

        public override void Open(TimeSpan timeout)
        {
            if (State != CommunicationState.Created && State != CommunicationState.Closed)
                throw new InvalidOperationException(string.Format("Cannot open the channel from the {0} state.", base.State));

            OnOpening();
#if VERBOSE
            DebugHelper.Start();
#endif
            //Create a queue for messages destined to this service, bind it to the service URI routing key
            //string queue = m_model.QueueDeclare();
            string queue = m_model.QueueDeclare(base.LocalAddress.Uri.PathAndQuery.Replace("/", ""), true, false, false, null);
            m_model.QueueBind(queue, Exchange, base.LocalAddress.Uri.PathAndQuery, null);

            //Listen to the queue
            m_messageQueue = new QueueingBasicConsumer(m_model);
            m_model.BasicQos(0, (ushort)m_bindingElement.BasicQOS, false);
            m_model.BasicConsume(queue, false, m_messageQueue);

#if VERBOSE
            DebugHelper.Stop(" ## In.Channel.Open {{\n\tAddress={1}, \n\tTime={0}ms}}.", LocalAddress.Uri.PathAndQuery);
#endif
            OnOpened();
        }
    }
}
