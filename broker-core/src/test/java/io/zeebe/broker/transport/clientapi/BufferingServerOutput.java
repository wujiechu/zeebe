/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.transport.clientapi;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.io.DirectBufferInputStream;
import org.agrona.sbe.MessageDecoderFlyweight;

import io.zeebe.protocol.clientapi.ControlMessageResponseDecoder;
import io.zeebe.protocol.clientapi.ErrorResponseDecoder;
import io.zeebe.protocol.clientapi.MessageHeaderDecoder;
import io.zeebe.test.broker.protocol.MsgPackHelper;
import io.zeebe.transport.ServerOutput;
import io.zeebe.transport.ServerResponse;
import io.zeebe.transport.TransportMessage;
import io.zeebe.transport.impl.RequestResponseHeaderDescriptor;
import io.zeebe.transport.impl.TransportHeaderDescriptor;

public class BufferingServerOutput implements ServerOutput
{

    protected final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    protected final ErrorResponseDecoder errorDecoder = new ErrorResponseDecoder();

    protected final MsgPackHelper msgPackDecoder = new MsgPackHelper();

    protected List<DirectBuffer> sentResponses = new CopyOnWriteArrayList<>();

    @Override
    public boolean sendMessage(TransportMessage transportMessage)
    {
        // ignore; not yet implemented
        return true;
    }

    @Override
    public boolean sendResponse(ServerResponse response)
    {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[response.getLength()]);
        response.write(buf, 0);
        sentResponses.add(buf);
        return true;
    }

    public List<DirectBuffer> getSentResponses()
    {
        return sentResponses;
    }

    public ErrorResponseDecoder getAsErrorResponse(int index)
    {
        return getAs(index, errorDecoder);
    }

    protected <T extends MessageDecoderFlyweight> T getAs(int index, T decoder)
    {
        final DirectBuffer sentResponse = sentResponses.get(index);
        final int offset = TransportHeaderDescriptor.HEADER_LENGTH + RequestResponseHeaderDescriptor.HEADER_LENGTH;
        headerDecoder.wrap(sentResponse, offset);

        decoder.wrap(sentResponse, offset + headerDecoder.encodedLength(), headerDecoder.blockLength(), headerDecoder.version());

        return decoder;
    }

    public Map<String, Object> getAsControlMessageData(int index)
    {
        final ControlMessageResponseDecoder decoder = getAs(index, new ControlMessageResponseDecoder());
        final UnsafeBuffer dataBuf = new UnsafeBuffer(new byte[decoder.dataLength()]);
        decoder.getData(dataBuf, 0, dataBuf.capacity());
        return msgPackDecoder.readMsgPack(new DirectBufferInputStream(dataBuf));
    }
}
